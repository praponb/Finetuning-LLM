SELECT *, CEIL(price) AS price_whole FROM products WHERE cost >= 700 OR supplier_id IS NULL ORDER BY id DESC
