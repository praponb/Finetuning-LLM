SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END quantity_bucket, SUM(quantity * unit_price) revenue FROM order_items GROUP BY 1 ORDER BY revenue DESC;
