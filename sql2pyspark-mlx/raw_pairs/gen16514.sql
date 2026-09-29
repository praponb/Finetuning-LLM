SELECT o.status, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date <= '2024-11-01'
GROUP BY o.status
HAVING SUM(oi.unit_price) > 2400
