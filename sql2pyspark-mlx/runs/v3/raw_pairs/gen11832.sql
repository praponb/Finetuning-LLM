WITH filtered_products AS (
    SELECT *, stock_qty - price AS stock_qty_minus_price
    FROM products
    WHERE brand != 'Globex'
),
products_stats AS (
    SELECT brand, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list, COUNT(price) AS price_count
    FROM filtered_products
    GROUP BY brand
)
SELECT brand, price_count
FROM products_stats
WHERE price_count >= 6
ORDER BY price_count DESC
