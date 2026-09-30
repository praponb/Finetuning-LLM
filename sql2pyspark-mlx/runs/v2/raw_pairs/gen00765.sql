SELECT
    id,
    brand,
    product_name,
    UPPER(product_name) AS product_name_upper,
    CONCAT(product_name, '/', category) AS product_name_category
FROM products
WHERE MONTH(launch_date) IN (7, 8, 12) AND category IN ('Grocery', 'Sports') AND supplier_id IN (18, 30);
