SELECT ss.id, ss.revenue, p.product_name, p.brand, p.category FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id WHERE p.price >= 880 AND ss.product_id = 8 ORDER BY ss.revenue DESC;
