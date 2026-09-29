WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        ADD_MONTHS(claim_date, 1) AS claim_date_plus_1m,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.product AS policy_product,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM prepared_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM combined_details_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount < 14377.6
              AND c.status = 'REJECTED'
        )
      AND UPPER(TRIM(s.status_clean)) LIKE 'I%'
)
SELECT
    adjuster_id,
    claim_type,
    policy_product,
    COUNT(*) AS cnt,
    SUM(adjuster_level) AS total_adjuster_level
FROM qualified
GROUP BY ROLLUP(adjuster_id, claim_type, policy_product)
