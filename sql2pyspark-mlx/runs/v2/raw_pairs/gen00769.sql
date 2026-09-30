SELECT COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count, COUNT(*) AS account_count
FROM accounts
WHERE account_type = 'SAVINGS' AND opened_date BETWEEN '2024-11-01' AND '2024-12-01' AND status = 'CLOSED'
ORDER BY open_count;
