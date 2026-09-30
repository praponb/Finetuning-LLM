WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM accounts
    EXCEPT
    SELECT status, id
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    status,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    COUNT(*) FILTER (WHERE account_type = 'CREDIT') AS credit_rows,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles_2,
    GROUPING(status) AS grouping_id
FROM verified
GROUP BY CUBE (status)
HAVING distinct_ids >= 1
ORDER BY distinct_ids NULLS FIRST
