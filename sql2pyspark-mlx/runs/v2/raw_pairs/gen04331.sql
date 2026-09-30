SELECT QUARTER(opened_date) AS opened_quarter, COUNT(balance) AS balance_count
FROM accounts
GROUP BY opened_quarter
HAVING balance_count > 1
ORDER BY balance_count;
