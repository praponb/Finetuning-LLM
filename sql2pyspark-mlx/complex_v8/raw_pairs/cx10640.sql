WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        ROUND(approved_amount * 1.1, 2) AS approved_amount_adj,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 13
      AND claim_type = 'fire'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT claim_type, COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM filtered_claims
    WHERE days_since_reported_at BETWEEN 140 AND 560
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE distinct_policy_ids < (SELECT MAX(distinct_policy_ids) FROM totals)
)
SELECT
    claim_type,
    distinct_policy_ids,
    FLOOR(distinct_policy_ids / 2) * 2 AS distinct_policy_ids_bucket
FROM filtered
WHERE distinct_policy_ids > 3
ORDER BY distinct_policy_ids_bucket DESC, claim_type
LIMIT 5
