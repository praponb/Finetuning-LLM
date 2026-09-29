WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        cost,
        launch_date,
        tags,
        CEIL(cost) AS cost_whole,
        CEIL(price) AS price_whole
    FROM products
    WHERE launch_date > DATE '2023-03-29'
)
-- Window metrics over several different partitions
SELECT
    id,
    cost_whole,
    price_whole,
    MIN(cost_whole) OVER (PARTITION BY category) AS category_min_cost_whole,
    LEAD(cost, 1, 0) OVER (PARTITION BY brand, category ORDER BY price_whole, id) AS next_cost,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_whole DESC, id) AS seq_no
FROM cleaned
