WITH customer_stats AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) status_list, COUNT(*) num_accounts
    FROM accounts
    GROUP BY customer_id
)
SELECT c.is_active, cs.status_list, cs.num_accounts
FROM customers c
LEFT OUTER JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.num_accounts >= 4
ORDER BY cs.num_accounts DESC;
