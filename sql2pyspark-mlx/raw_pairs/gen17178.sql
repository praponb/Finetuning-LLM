SELECT
    id,
    category,
    price,
    cost,
    FIRST_VALUE(cost) OVER (PARTITION BY category ORDER BY price, id) AS first_cost
FROM products
