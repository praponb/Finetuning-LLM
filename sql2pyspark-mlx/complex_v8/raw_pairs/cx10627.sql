WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status
    FROM claims
    WHERE claim_type NOT IN ('fire', 'collision')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM cleaned)
)
SELECT
    claim_type,
    adjuster_id,
    status,
    COUNT(*) AS n_rows,
    ROUND(AVG(approved_amount), 2) AS avg_approved_amount,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM screened
GROUP BY ROLLUP(claim_type, adjuster_id, status)
