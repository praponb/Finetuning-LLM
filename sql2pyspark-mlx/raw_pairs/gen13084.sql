WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_ADD(last_restock, 58) last_restock_plus_58d,
        DATE_TRUNC('YEAR', last_restock) last_restock_period
    FROM inventory
    WHERE last_restock >= DATE '2023-04-02'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        last_restock_plus_58d,
        last_restock_period,
        MIN(quantity) OVER w_ordered running_min_quantity,
        MAX(quantity) OVER w_ordered running_max_quantity,
        AVG(quantity) OVER w_ordered running_avg_quantity,
        SUM(quantity) OVER w_ordered running_sum_quantity
    FROM cleaned
    WHERE MONTH(last_restock_plus_58d) IN (3, 7, 8)
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_max_quantity DESC NULLS FIRST, id) AS row_num
        FROM scored
    ) t
    WHERE row_num = 1
),
latest_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_58d DESC, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        last_restock_period,
        running_min_quantity,
        running_avg_quantity,
        running_sum_quantity,
        row_num,
        LAG(running_avg_quantity) OVER w_hist prev_running_avg_quantity,
        LEAD(running_avg_quantity) OVER w_hist next_running_avg_quantity,
        LAG(running_max_quantity) OVER w_hist prev_running_max_quantity,
        LEAD(running_max_quantity) OVER w_hist next_running_max_quantity,
        ROW_NUMBER() OVER w_hist event_seq
    FROM latest_inventory
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY running_min_quantity, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, MONTH(last_restock_period) last_restock_period_month
    FROM enriched
    WHERE product_id IN (2, 3, 5, 8)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    p.brand,
    COUNT(*) AS rows_total,
    ROUND(SUM(a.prev_running_avg_quantity), 2) AS total_prev_running_avg_quantity
FROM filtered a
INNER JOIN products p ON a.product_id = p.id
GROUP BY p.category, p.brand
