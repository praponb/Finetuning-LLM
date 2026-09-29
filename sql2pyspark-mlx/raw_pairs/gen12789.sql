SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, COUNT(ss.units) AS units_count
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
