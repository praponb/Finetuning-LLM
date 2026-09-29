SELECT id, price, brand, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE category NOT IN ('Fashion', 'Electronics', 'Home');
