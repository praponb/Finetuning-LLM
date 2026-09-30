SELECT opened_date, customer_id
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN')
UNION
SELECT opened_date, customer_id
FROM accounts
WHERE account_type IN ('LOAN', 'CHECKING')
ORDER BY customer_id;
