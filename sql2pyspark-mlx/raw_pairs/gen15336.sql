WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CASE WHEN units >= 43 THEN 'High' WHEN units >= 11 THEN 'Medium' ELSE 'Low' END AS units_tier
    FROM store_sales
    WHERE units < 17
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.units,
        src.revenue,
        src.units_tier,
        d.city
    FROM filtered src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY revenue DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
filtered_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(row_num * 1.15, 2) AS row_num_scaled,
        row_num * units AS row_num_times_units
    FROM deduped
    WHERE city IS NOT NULL
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_num_scaled) OVER w_running AS running_sum_row_num_scaled,
        AVG(row_num_scaled) OVER w_running AS running_avg_row_num_scaled,
        MAX(row_num_scaled) OVER w_running AS running_max_row_num_scaled
    FROM filtered_store_sales
    WINDOW w_running AS (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_sum_row_num_scaled >= 310 THEN 'HIGH' ELSE 'LOW' END AS running_sum_row_num_scaled_tier,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        AVG(running_sum_row_num_scaled) FILTER (WHERE city = 'Bangkok') AS bangkok_avg_running_sum_row_num_scaled
    FROM enriched_store_sales
    GROUP BY CASE WHEN running_sum_row_num_scaled >= 310 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    running_sum_row_num_scaled_tier,
    row_count,
    bangkok_avg_running_sum_row_num_scaled
FROM summary
ORDER BY running_sum_row_num_scaled_tier NULLS LAST;
