WITH AccountsByCustomer AS (
    SELECT customer_id, ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_open, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.age,
    A.pct_open,
    A.status_values
FROM customers c
LEFT JOIN AccountsByCustomer A ON c.id = A.customer_id
ORDER BY c.age DESC, A.pct_open
