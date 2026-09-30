SELECT a.id, a.product_name, a.brand, a.stock_qty, a.tags, a.supplier_id, d.city, d.country FROM products a LEFT JOIN suppliers d ON a.supplier_id = d.id
