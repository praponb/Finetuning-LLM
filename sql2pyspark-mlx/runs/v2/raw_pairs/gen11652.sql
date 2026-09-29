SELECT status, YEAR(opened_date) AS opened_year, SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count
FROM accounts
GROUP BY status, YEAR(opened_date)
ORDER BY status ASC;
