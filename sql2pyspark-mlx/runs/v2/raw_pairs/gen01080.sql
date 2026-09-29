SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, s.region, SUM(i.quantity) AS total_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE s.sqft >= 11920
GROUP BY DATE_TRUNC('MONTH', s.opened_date), s.region
