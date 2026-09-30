SELECT
    c.country,
    c.is_active,
    ROUND(STDDEV(a.balance), 2) AS stddev_balance,
    SORT_ARRAY(COLLECT_SET(a.status)) AS status_list
FROM accounts a
RIGHT OUTER JOIN customers c ON a.customer_id = c.id
GROUP BY c.country, c.is_active;
