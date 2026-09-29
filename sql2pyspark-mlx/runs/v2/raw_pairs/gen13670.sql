SELECT
    c.country,
    c.city,
    SUM(a.balance) AS total_balance,
    ROUND((100 * SUM(CASE WHEN a.account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM accounts a
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE c.country = 'SG'
GROUP BY c.country, c.city
ORDER BY pct_credit DESC, total_balance;
