SELECT
    id,
    product_name,
    price,
    tags,
    SIZE(tags) AS tags_count
FROM products;
