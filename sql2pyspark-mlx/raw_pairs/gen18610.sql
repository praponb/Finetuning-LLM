SELECT QUARTER(o.order_date) AS order_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.method))) AS method_values, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE o.channel IS NOT NULL
GROUP BY QUARTER(o.order_date)
ORDER BY method_values DESC, order_quarter DESC;
