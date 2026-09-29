SELECT
    MONTH(accounts.opened_date) AS opened_month,
    accounts.account_type,
    ROUND(AVG(transactions.amount), 2) AS avg_amount,
    MAX(transactions.amount) AS max_amount
FROM transactions
JOIN accounts ON transactions.account_id = accounts.id
GROUP BY MONTH(accounts.opened_date), accounts.account_type
HAVING ROUND(AVG(transactions.amount), 2) > 22
