SELECT ss.id, ss.units, ss.sale_date
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE LOWER(product_name) LIKE '%esk%');
