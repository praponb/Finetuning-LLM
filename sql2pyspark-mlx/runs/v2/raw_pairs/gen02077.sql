SELECT
    MONTH(a.opened_date) AS opened_month,
    YEAR(a.opened_date) AS opened_year,
    COUNT(DISTINCT t.amount) AS unique_accounts,
    COUNT(DISTINCT t.amount) AS unique_merchants
FROM transactions t
JOIN accounts a ON t.account_id = a.id
GROUP BY MONTH(a.opened_date), YEAR(a.opened_date)
ORDER BY opened_month ASC;
