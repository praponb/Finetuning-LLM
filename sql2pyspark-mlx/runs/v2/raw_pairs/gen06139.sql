SELECT
    id,
    price,
    supplier_id,
    SIZE(tags) AS tags_count
FROM products;
