SELECT products.category, inventory.product_id, DATE_FORMAT(products.launch_date, 'MMM yyyy') AS launch_date_label
FROM products
JOIN inventory ON inventory.product_id = products.id
WHERE products.price > 930
ORDER BY inventory.product_id DESC, launch_date_label DESC;
