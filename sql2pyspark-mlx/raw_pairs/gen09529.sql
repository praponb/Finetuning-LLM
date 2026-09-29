SELECT
    a.id,
    a.opened_date,
    a.balance,
    t.stddev_amount
FROM accounts a
LEFT OUTER JOIN (SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY stddev_amount DESC;
