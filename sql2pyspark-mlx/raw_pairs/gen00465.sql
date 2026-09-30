SELECT
    id,
    cost,
    COALESCE(stock_qty, 0) AS stock_qty_filled,
    stock_qty - price AS stock_qty_minus_price
FROM products
WHERE product_name IS NULL OR product_name LIKE '%Yoga%';
