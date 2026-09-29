SELECT oi.id, oi.product_id, p.brand, p.price, p.stock_qty, ROUND(oi.unit_price, 1) AS unit_price_rounded FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id
