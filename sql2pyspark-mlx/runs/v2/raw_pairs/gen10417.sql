WITH customer_totals AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM accounts
    GROUP BY customer_id
)
SELECT c.email, ct.status_list
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
ORDER BY email
