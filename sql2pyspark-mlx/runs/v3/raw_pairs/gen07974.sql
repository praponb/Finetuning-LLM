SELECT QUARTER(o.order_date) AS order_quarter, o.customer_id, MIN(s.shipped_date) AS min_shipped_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY QUARTER(o.order_date), o.customer_id
ORDER BY min_shipped_date;
