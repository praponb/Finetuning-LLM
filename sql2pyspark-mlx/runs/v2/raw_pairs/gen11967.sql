SELECT
    launch_quarter,
    brand,
    avg_price,
    total_cost
FROM (
    SELECT
        QUARTER(launch_date) AS launch_quarter,
        brand,
        AVG(price) AS avg_price,
        SUM(cost) AS total_cost
    FROM products
    GROUP BY QUARTER(launch_date), brand
) AS t
WHERE total_cost < 3200;
