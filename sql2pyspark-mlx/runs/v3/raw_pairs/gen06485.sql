SELECT
    id,
    price,
    supplier_id,
    CONCAT(product_name, ' ', brand) AS product_name_brand,
    CASE WHEN category IN ('Fashion', 'Grocery') THEN 'Priority' ELSE 'Other' END AS category_group
FROM products;
