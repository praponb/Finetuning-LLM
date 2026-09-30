SELECT
    id,
    brand,
    supplier_id,
    launch_date,
    SUBSTRING(product_name, 1, 3) product_name_prefix
FROM products
WHERE stock_qty BETWEEN 174 AND 357;
