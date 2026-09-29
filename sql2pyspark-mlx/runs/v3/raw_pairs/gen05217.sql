SELECT s.region, MONTH(s.opened_date) AS opened_month, MAX(o.shipping_fee) - MIN(o.shipping_fee) AS shipping_fee_range
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
GROUP BY s.region, MONTH(s.opened_date)
ORDER BY shipping_fee_range DESC, region ASC, opened_month ASC
LIMIT 50
