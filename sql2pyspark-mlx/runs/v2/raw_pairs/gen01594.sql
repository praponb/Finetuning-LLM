SELECT
    s.region,
    DATE_TRUNC('MONTH', s.opened_date) AS opened_month,
    MAX(i.quantity) AS max_quantity,
    ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN stores s ON s.id = i.store_id
WHERE i.store_id IN (3, 4)
GROUP BY s.region, DATE_TRUNC('MONTH', s.opened_date)
HAVING MAX(i.quantity) >= 90
ORDER BY max_quantity DESC;
