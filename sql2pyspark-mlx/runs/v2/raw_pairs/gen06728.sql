WITH accounts_by_customer AS (
    SELECT customer_id, COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count
    FROM accounts
    GROUP BY customer_id
)
SELECT c.id, c.name, abc.checking_count
FROM customers c
LEFT JOIN accounts_by_customer abc ON c.id = abc.customer_id
