SELECT
    category,
    SUM(price) AS total_price,
    SUM(CASE WHEN brand = 'Stark' THEN stock_qty ELSE 0 END) AS stark_stock_qty,
    SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END) AS home_count
FROM products
WHERE product_name LIKE '%Bac%'
GROUP BY 1;
