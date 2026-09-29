SELECT
    id,
    price,
    brand,
    launch_date
FROM products
WHERE price >= (SELECT AVG(price) FROM products WHERE brand IN ('Initech', 'Acme'))
ORDER BY brand, id DESC
