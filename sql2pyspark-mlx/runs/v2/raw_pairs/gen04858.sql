SELECT id, account_type FROM accounts WHERE MONTH(opened_date) IN (3, 7, 12) INTERSECT SELECT id, account_type FROM accounts WHERE customer_id IN (1, 24, 37)
