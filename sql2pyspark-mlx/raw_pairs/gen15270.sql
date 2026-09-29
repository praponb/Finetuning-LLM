SELECT status, LOWER(status) AS status_lower
FROM accounts
WHERE balance < 58000
ORDER BY status_lower DESC;
