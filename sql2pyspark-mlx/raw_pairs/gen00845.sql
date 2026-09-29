SELECT
    id,
    brand,
    launch_date,
    price,
    LAG(price, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_price
FROM products
