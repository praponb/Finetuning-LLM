SELECT CASE WHEN products.price >= 1000 THEN 'high' WHEN products.price >= 210 THEN 'mid' ELSE 'low' END AS price_bucket, ROUND(AVG(order_items.quantity), 2) AS avg_quantity
FROM order_items
FULL OUTER JOIN products ON order_items.product_id = products.id
FULL OUTER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN products.price >= 1000 THEN 'high' WHEN products.price >= 210 THEN 'mid' ELSE 'low' END;
