SELECT YEAR(a.opened_date) opened_year, DATE_FORMAT(a.opened_date, 'yyyy-MM') opened_month, SUM(t.amount) total_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY YEAR(a.opened_date), DATE_FORMAT(a.opened_date, 'yyyy-MM')
