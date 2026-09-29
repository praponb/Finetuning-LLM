SELECT *
FROM (
    SELECT brand, COUNT(*) AS num_products, ROUND(STDDEV(price), 2) AS stddev_price
    FROM products
    GROUP BY brand
) AS x
WHERE stddev_price > 21;
