WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        launch_date,
        MAX(price) OVER w_recent AS running_max_price,
        SUM(price) OVER w_recent AS running_sum_price,
        COUNT(price) OVER w_recent AS running_count_price,
        LEAD(cost, 1, 0) OVER (ORDER BY price, id) AS next_cost
    FROM products
    WHERE stock_qty != 299
    WINDOW w_recent AS (PARTITION BY brand, supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    product_name,
    category,
    cost,
    stock_qty,
    launch_date,
    running_max_price,
    running_sum_price,
    running_count_price
FROM with_windows
ORDER BY running_sum_price DESC;
