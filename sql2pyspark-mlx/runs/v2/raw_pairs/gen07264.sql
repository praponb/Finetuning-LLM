SELECT
    brand,
    category,
    AVG(cost) AS avg_cost,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
WHERE product_name IS NULL
GROUP BY brand, category
ORDER BY brand
