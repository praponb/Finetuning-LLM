SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON s.id = o.store_id
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
ORDER BY stddev_amount, opened_month
LIMIT 50
