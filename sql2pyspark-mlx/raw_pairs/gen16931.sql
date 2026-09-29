SELECT COUNT(amount) AS amount_count
FROM transactions
WHERE account_id IN (13, 34, 35) AND merchant IS NULL
ORDER BY amount_count DESC
