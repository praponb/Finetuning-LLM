SELECT QUARTER(p.launch_date) AS launch_quarter, MAX(ss.units) - MIN(ss.units) AS units_range, MAX(ss.sale_date) AS last_sale_date
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY QUARTER(p.launch_date)
