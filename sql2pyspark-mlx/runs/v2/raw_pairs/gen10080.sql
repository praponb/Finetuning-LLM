SELECT
    YEAR(opened_date) AS opened_year,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    COUNT(balance) AS balance_count,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count
FROM accounts
GROUP BY YEAR(opened_date)
HAVING COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) >= 3
ORDER BY stddev_balance DESC, closed_count;
