WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM recent
)
-- Subtotals with CUBE
SELECT
    status,
    claim_type,
    policy_id,
    ROUND(SUM(approved_amount), 2) AS total_approved_amount,
    COUNT(*) FILTER (WHERE claim_type = 'theft') AS n_claim_type_theft
FROM scored
GROUP BY CUBE(status, claim_type, policy_id)
ORDER BY status NULLS LAST, claim_type NULLS LAST, policy_id NULLS LAST
