WITH customer_totals AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM accounts
    GROUP BY customer_id
)
SELECT c.email, ct.account_type_list
FROM customers c
INNER JOIN customer_totals ct ON c.id = ct.customer_id;
