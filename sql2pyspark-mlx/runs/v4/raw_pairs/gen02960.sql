SELECT
    id,
    price,
    launch_date,
    cost,
    CONCAT(product_name, ' ', brand) AS product_name_brand,
    COALESCE(price, 0) AS price_filled
FROM products;
