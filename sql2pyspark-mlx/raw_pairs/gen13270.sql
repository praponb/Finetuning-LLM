SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM');
