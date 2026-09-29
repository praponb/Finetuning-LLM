WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount DESC, id) AS latest_rank
    FROM payments
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    method,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    MIN(amount) AS min_amount,
    MAX(paid_at) AS last_paid_at,
    GROUPING_ID() AS grouping_id
FROM latest
GROUP BY GROUPING SETS ((method, DATE_TRUNC('MONTH', paid_at)), (method), ())
HAVING MIN(amount) > 0
