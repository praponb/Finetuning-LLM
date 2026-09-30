WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
    FROM claims
    WHERE status IS NOT NULL
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('OPEN', 'REJECTED')
        )
      AND s.claim_date >= DATE '2024-04-15'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(claim_date) AS claim_date_year,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(claimed_amount) FILTER (WHERE claim_type = 'theft') AS claimed_amount_claim_type_theft,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        MIN(claim_date) AS first_claim_date,
        SUM(CASE WHEN claim_type <> 'flood' THEN 1 ELSE 0 END) AS n_claim_type_flood
    FROM filtered
    GROUP BY YEAR(claim_date)
)
-- Final projection
SELECT
    claim_date_year,
    claimed_amount_claim_type_theft,
    claim_date_span_days,
    first_claim_date,
    n_claim_type_flood
FROM per_group
WHERE distinct_ids <= 8
ORDER BY claim_date_year DESC, n_claim_type_flood
LIMIT 5
