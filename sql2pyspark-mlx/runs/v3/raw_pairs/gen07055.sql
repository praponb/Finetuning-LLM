SELECT products.launch_date, inventory.last_restock, ARRAY_JOIN(products.tags, ', ') AS tags_text
FROM products
LEFT JOIN inventory ON products.id = inventory.product_id
ORDER BY products.launch_date
