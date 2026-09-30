WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE start_date < DATE '2025-08-03'
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM recent_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product IN ('TRAVEL', 'AUTO')
        )
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM qualified s
    LEFT JOIN claims c ON c.policy_id = s.id
),
qualified_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_children_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date IS NOT NULL
              AND c.approved_amount < 32878.5
        )
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.id) AS avg_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM qualified_2 s
),
holder_id_stats AS (
    -- Avg claim_approved_amount per holder_id
    SELECT holder_id, AVG(claim_approved_amount) AS avg_claim_approved_amount_2
    FROM profiled
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_approved_amount_2,
        ROUND(s.claim_approved_amount - g.avg_claim_approved_amount_2, 2) AS claim_approved_amount_minus_avg
    FROM profiled s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.claim_approved_amount > g.avg_claim_approved_amount_2
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', first_claim_date) AS first_claim_date_year,
        ROUND(claim_count / NULLIF(claim_approved_amount, 0), 3) AS claim_count_per_claim_approved_amount,
        ROUND(claim_approved_amount / NULLIF(premium, 0), 3) AS claim_approved_amount_per_premium
    FROM above_average
    WHERE premium <= 3532.7
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    claim_id,
    claim_approved_amount,
    first_claim_date,
    claim_count,
    first_claim_date_year,
    claim_count_per_claim_approved_amount,
    claim_approved_amount_per_premium
FROM tagged
ORDER BY holder_id DESC, claim_approved_amount DESC NULLS FIRST
