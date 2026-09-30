SELECT *
FROM accounts
WHERE balance > 30000 OR customer_id = 17
ORDER BY id DESC
LIMIT 10;
