SELECT COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count, MIN(opened_date) AS first_opened_date
FROM accounts
WHERE balance < 54000
