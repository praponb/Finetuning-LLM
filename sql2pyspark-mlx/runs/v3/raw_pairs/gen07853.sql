WITH customer_stats AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.segment,
    c.country,
    cs.account_type_values,
    cs.closed_count
FROM customers c
JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.closed_count > 76
ORDER BY c.segment;
