SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, ROUND(STDDEV(o.discount), 2) AS stddev_discount
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
JOIN stores s ON s.id = o.store_id
WHERE o.discount >= 0.32
GROUP BY DATE_TRUNC('MONTH', s.opened_date);
