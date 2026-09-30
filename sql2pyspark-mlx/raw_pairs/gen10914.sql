WITH customer_summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, SUM(balance) AS total_balance
    FROM accounts
    WHERE account_type IN ('CREDIT', 'SAVINGS', 'CHECKING')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    cs.account_type_values,
    cs.total_balance
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.total_balance >= 284000
ORDER BY total_balance DESC, id
LIMIT 20;
