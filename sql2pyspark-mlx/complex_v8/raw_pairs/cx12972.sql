WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE end_date BETWEEN DATE '2025-12-05' AND DATE '2026-12-05'
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status_slug,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount
    FROM base_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
per_group AS (
    -- Summarise per group
    SELECT
        claim_adjuster_id,
        MAX(claim_approved_amount) AS max_claim_approved_amount,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(claim_approved_amount), 2) AS total_claim_approved_amount
    FROM line_level_claims
    WHERE claim_approved_amount < 15857.9
    GROUP BY 1
)
SELECT
    claim_adjuster_id,
    distinct_ids,
    total_claim_approved_amount,
    ABS(distinct_ids - 6) AS distinct_ids_distance
FROM per_group
WHERE distinct_ids < 7
ORDER BY distinct_ids DESC, total_claim_approved_amount
