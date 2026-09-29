SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
FROM accounts
WHERE balance <= 17000 AND (opened_date <= '2025-09-23' OR balance >= 29000)
ORDER BY account_type, opened_date, id
LIMIT 50;
