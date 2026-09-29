SELECT ss.id, ss.product_id, ss.store_id
FROM store_sales ss
WHERE ss.sale_date > '2023-04-06' AND ss.product_id NOT IN (SELECT id FROM products WHERE SIZE(tags) >= 1)
ORDER BY product_id DESC, 3 ASC;
