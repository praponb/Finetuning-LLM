WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant = 'Lazada'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 89000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    APPROX_PERCENTILE(balance, 0.9) AS balance_p90,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM kept
GROUP BY CASE WHEN balance >= 89000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_customer_ids NULLS LAST
