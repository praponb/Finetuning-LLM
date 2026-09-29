SELECT MONTH(s.opened_date) AS opened_month, COUNT(o.discount) AS discount_count, SUM(CASE WHEN o.channel = 'mobile' THEN o.discount ELSE 0 END) AS mobile_discount
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY MONTH(s.opened_date)
ORDER BY mobile_discount, opened_month DESC;
