WITH filtered AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        ROUND(stock_qty / price, 2) AS stock_qty_per_price,
        DATE_TRUNC('WEEK', launch_date) AS launch_date_period
    FROM products
    WHERE launch_date > TO_DATE('2023-07-27')
      AND supplier_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        stock_qty_per_price,
        launch_date_period,
        AVG(stock_qty_per_price) OVER (PARTITION BY category ORDER BY CAST(launch_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS stock_qty_per_price_avg_7d,
        LAG(price, 1) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_price,
        RANK() OVER (PARTITION BY brand ORDER BY stock_qty_per_price DESC NULLS FIRST) AS stock_qty_per_price_rank,
        AVG(cost) OVER (PARTITION BY MONTH(launch_date), brand ORDER BY launch_date_period, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM filtered
    WHERE brand = 'Initech'
),
valid AS (
    SELECT
        *,
        ROUND(stock_qty_per_price_rank * 100, 2) AS stock_qty_per_price_rank_adjusted,
        ROUND(cost, 1) AS cost_rounded
    FROM scored
    WHERE category = 'Electronics'
),
scored_products AS (
    SELECT
        id,
        category,
        cost,
        stock_qty,
        launch_date,
        stock_qty_per_price,
        launch_date_period,
        stock_qty_per_price_avg_7d,
        prev_price,
        stock_qty_per_price_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY TO_DATE(launch_date_period), category), 4) AS price_share,
        LAG(cost_avg_2_prior, 1, 0) OVER (PARTITION BY category ORDER BY cost_avg_2_prior, id) AS prev_cost_avg_2_prior,
        SUM(cost_avg_2_prior) OVER (PARTITION BY category ORDER BY launch_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_avg_2_prior,
        LEAD(stock_qty, 1, 0) OVER (PARTITION BY DATE_TRUNC('month', launch_date_period), category ORDER BY stock_qty_per_price, id) AS next_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS event_seq
    FROM valid
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY prev_cost_avg_2_prior DESC, id) AS rank_in_group
        FROM scored_products
    ) t
    WHERE rank_in_group = 1
)
SELECT
    id,
    category,
    cost,
    stock_qty,
    stock_qty_per_price,
    launch_date_period,
    stock_qty_per_price_rank,
    prev_cost_avg_2_prior,
    next_stock_qty
FROM ranked
ORDER BY id NULLS FIRST
