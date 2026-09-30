WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE YEAR(end_date) = 2026
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id
    FROM scoped_policys s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.adjuster_id >= 3
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.claim_id,
        s.claim_date,
        s.claim_adjuster_id,
        c.id AS claim_id_2,
        c.claim_date AS claim_claim_date,
        c.status AS claim_status_2,
        c.reported_at AS claim_reported_at_2,
        c.approved_amount AS claim_approved_amount
    FROM with_children_claims s
    JOIN claims c ON c.policy_id = s.id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        claim_id_2,
        holder_id,
        COUNT(*) AS cnt,
        MIN(claim_reported_at_2) AS first_claim_reported_at_2,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(claim_approved_amount) / NULLIF(SUM(premium), 0), 3) AS claim_approved_amount_to_premium_ratio
    FROM line_level
    GROUP BY claim_id_2, holder_id
),
with_parent_claim AS (
    -- Join claims
    SELECT
        s.claim_id_2,
        s.holder_id,
        s.cnt,
        s.first_claim_reported_at_2,
        s.status_list,
        s.distinct_ids,
        s.claim_approved_amount_to_premium_ratio,
        c.approved_amount AS claim_approved_amount,
        c.claim_date,
        c.reported_at AS claim_reported_at
    FROM rollup_base s
    JOIN claims c ON c.id = s.claim_id_2
)
SELECT
    claim_id_2,
    holder_id,
    first_claim_reported_at_2,
    status_list,
    distinct_ids,
    claim_approved_amount_to_premium_ratio,
    claim_approved_amount,
    FLOOR(cnt / 2) * 2 AS cnt_bucket
FROM with_parent_claim
WHERE distinct_ids < 9
ORDER BY holder_id DESC, claim_id_2
LIMIT 10
