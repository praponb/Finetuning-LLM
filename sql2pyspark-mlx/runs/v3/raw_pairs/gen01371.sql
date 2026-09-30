SELECT
    id,
    cost,
    category,
    LOWER(category) AS category_lower,
    INITCAP(TRIM(category)) AS category_title
FROM products
WHERE stock_qty > 140 AND SIZE(tags) > 2
