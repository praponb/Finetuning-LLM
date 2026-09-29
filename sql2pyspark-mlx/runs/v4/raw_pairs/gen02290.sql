SELECT
    c.is_active,
    YEAR(c.signup_date) AS signup_year,
    AVG(a.balance) AS avg_balance,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(a.status))) AS status_values
FROM accounts a
JOIN customers c ON a.customer_id = c.id
GROUP BY c.is_active, YEAR(c.signup_date)
ORDER BY status_values DESC, is_active ASC, signup_year ASC
LIMIT 5;
