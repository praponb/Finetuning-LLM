SELECT *
FROM accounts
WHERE status != 'CLOSED' AND (opened_date BETWEEN '2024-06-23' AND '2024-11-01' OR status IN ('FROZEN', 'OPEN', 'CLOSED'));
