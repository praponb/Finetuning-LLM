SELECT *
FROM (
    SELECT category, MIN(cost) AS min_cost
    FROM products
    WHERE price BETWEEN 790 AND 1160
    GROUP BY category
) AS t
WHERE min_cost < 520
