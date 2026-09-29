SELECT p.supplier_id, p.stock_qty, p.category, oi.quantity, CAST(oi.quantity AS DOUBLE) quantity_num FROM products p JOIN order_items oi ON oi.product_id = p.id ORDER BY 4 DESC;
