SELECT opened_date
FROM accounts
WHERE account_type = 'CREDIT'
EXCEPT
SELECT last_restock
FROM inventory
WHERE quantity BETWEEN 75 AND 142;
