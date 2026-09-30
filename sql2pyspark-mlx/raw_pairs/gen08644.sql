SELECT
    p.id,
    p.price,
    p.cost,
    t.min_units,
    t.stddev_units
FROM products p
JOIN (SELECT product_id, MIN(units) AS min_units, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE store_id IN (3, 4, 5, 6)
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.stddev_units > 23
ORDER BY id, stddev_units;
