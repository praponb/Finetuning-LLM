SELECT p.supplier_id, MAX(ss.units) - MIN(ss.units) units_range, MAX(ss.revenue) - MIN(ss.revenue) revenue_range
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
GROUP BY p.supplier_id
HAVING MAX(ss.units) - MIN(ss.units) >= 33
ORDER BY supplier_id DESC;
