SELECT
    status,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    MAX(opened_date) AS last_opened_date,
    MAX(opened_date) AS latest_opened_date
FROM accounts
GROUP BY status
HAVING stddev_balance < 70;
