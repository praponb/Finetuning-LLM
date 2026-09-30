WITH customer_summary AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) status_list, SUM(discount) total_discount
    FROM orders
    WHERE discount BETWEEN 0.16 AND 0.32
    GROUP BY customer_id
)
SELECT
    c.id,
    c.country,
    cs.status_list,
    cs.total_discount
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY id, cs.total_discount DESC;
