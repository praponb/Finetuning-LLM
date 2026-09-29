WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-10'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue_per_unit) OVER w_ordered AS running_avg_revenue_per_unit,
        MIN(revenue_per_unit) OVER w_ordered AS running_min_revenue_per_unit,
        COUNT(revenue_per_unit) OVER w_ordered AS running_count_revenue_per_unit
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_avg_revenue_per_unit DESC, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        e.running_min_revenue_per_unit,
        e.running_count_revenue_per_unit,
        e.row_num,
        s.store_name,
        s.sqft
    FROM deduped e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
deduped_store_sales AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        store_name
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) = 1
)
-- Grouped metrics
SELECT
    product_id,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(*) AS num_rows_2
FROM deduped_store_sales
GROUP BY product_id;
