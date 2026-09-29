SELECT CASE WHEN p.stock_qty >= 446 THEN 'high' WHEN p.stock_qty >= 107 THEN 'mid' ELSE 'low' END stock_qty_bucket, DATE_FORMAT(p.launch_date, 'yyyy-MM') launch_month, MAX(i.last_restock) latest_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.stock_qty >= 446 THEN 'high' WHEN p.stock_qty >= 107 THEN 'mid' ELSE 'low' END, DATE_FORMAT(p.launch_date, 'yyyy-MM')
