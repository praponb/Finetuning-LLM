SELECT
    id,
    category,
    ROUND(cost * 0.85, 2) AS cost_adjusted,
    CONCAT_WS('/', product_name, category) AS product_name_category_key
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance') AND (SIZE(tags) >= 1 OR price > 600);
