WITH scored AS (
    SELECT
        id,
        cost,
        stock_qty,
        supplier_id,
        COUNT(cost) OVER w_recent AS last7_count_cost,
        MIN(cost) OVER w_recent AS last7_min_cost,
        SUM(cost) OVER w_recent AS last7_sum_cost
    FROM products
    WHERE YEAR(launch_date) = 2024
    WINDOW w_recent AS (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    stock_qty,
    last7_sum_cost
FROM scored
ORDER BY stock_qty NULLS LAST, last7_sum_cost DESC;
