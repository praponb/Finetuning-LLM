SELECT products.id, products.category, products.price, products.stock_qty FROM products WHERE products.cost <= 710 AND products.id IN (SELECT product_id FROM order_items) ORDER BY products.id DESC
