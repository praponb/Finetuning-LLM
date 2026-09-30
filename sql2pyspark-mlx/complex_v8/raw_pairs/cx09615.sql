WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE (product <> 'TRAVEL' OR end_date BETWEEN DATE '2026-01-20' AND DATE '2027-05-16')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(end_date) OVER (PARTITION BY product, status ORDER BY premium ASC, id) AS prev_end_date,
        RANK() OVER (PARTITION BY product, status ORDER BY premium ASC) AS premium_rank
    FROM filtered_policys
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.prev_end_date,
        c.id AS claim_id,
        c.claim_date,
        c.claim_type,
        c.approved_amount AS claim_approved_amount
    FROM windowed s
    JOIN claims c ON c.policy_id = s.id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM detail_rows_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.reported_at < TIMESTAMP '2025-02-18 09:00:00'
              AND c.adjuster_id IS NOT NULL
        )
),
with_activity_2 AS (
    -- Keep rows whose claim matches
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.id = s.claim_id
              AND c.status = 'PAID'
              AND c.reported_at >= TIMESTAMP '2025-01-25 08:00:00'
        )
),
status_stats AS (
    -- Max premium per status
    SELECT status, MAX(premium) AS max_premium
    FROM with_activity_2
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM with_activity_2 s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium = g.max_premium
),
with_parent_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.prev_end_date,
        s.claim_id,
        s.claim_date,
        s.claim_type,
        s.claim_approved_amount,
        s.premium_minus_max,
        c.adjuster_id AS claim_adjuster_id
    FROM outliers s
    LEFT JOIN claims c ON c.id = s.claim_id
)
-- Subtotals with GROUPING SETS
SELECT
    claim_adjuster_id,
    COALESCE(claim_type, 'ALL') AS claim_type,
    MAX(claim_date) AS last_claim_date,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM with_parent_claim
GROUP BY GROUPING SETS ((claim_adjuster_id, claim_type), (claim_adjuster_id), ())
