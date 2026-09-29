SELECT id, customer_id, ROUND(balance * 1.15, 2) AS balance_scaled
FROM accounts
WHERE balance > 22000 AND (status = 'FROZEN' OR balance < 33000)
