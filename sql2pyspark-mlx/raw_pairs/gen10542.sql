WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        GREATEST(revenue, units) AS greatest_revenue_units,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE store_id IN (1, 2, 4, 6)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(greatest_revenue_units) OVER (PARTITION BY product_id, store_id ORDER BY greatest_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_greatest_revenue_units,
        AVG(greatest_revenue_units) OVER (PARTITION BY product_id, store_id ORDER BY greatest_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_greatest_revenue_units,
        SUM(greatest_revenue_units) OVER (PARTITION BY product_id, store_id ORDER BY greatest_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_greatest_revenue_units
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_avg_greatest_revenue_units,
        running_sum_greatest_revenue_units,
        LEAD(units) OVER (PARTITION BY store_id, product_id ORDER BY running_sum_greatest_revenue_units, id) AS next_units,
        LAG(running_avg_greatest_revenue_units) OVER (PARTITION BY store_id, product_id ORDER BY running_sum_greatest_revenue_units, id) AS prev_running_avg_greatest_revenue_units,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY running_sum_greatest_revenue_units, id) AS seq_no
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_hist AS running_avg_greatest_revenue_units_cume_dist,
        PERCENT_RANK() OVER (ORDER BY running_sum_greatest_revenue_units DESC NULLS LAST) AS running_sum_greatest_revenue_units_percent_rank
    FROM scored
    WHERE prev_running_avg_greatest_revenue_units < 22
    WINDOW w_hist AS (ORDER BY running_avg_greatest_revenue_units DESC NULLS FIRST)
),
with_windows_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(running_sum_greatest_revenue_units_percent_rank) IGNORE NULLS OVER w_grp AS first_known_running_sum_greatest_revenue_units_percent_rank,
        SUM(prev_running_avg_greatest_revenue_units) OVER w_grp AS cumulative_prev_running_avg_greatest_revenue_units
    FROM metrics
    WINDOW w_grp AS (ORDER BY running_sum_greatest_revenue_units_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(seq_no AS STRING) AS seq_no_str
    FROM with_windows_store_sales
    WHERE running_avg_greatest_revenue_units_cume_dist > 0.54
)
SELECT
    id,
    running_avg_greatest_revenue_units,
    running_sum_greatest_revenue_units,
    next_units,
    prev_running_avg_greatest_revenue_units,
    first_known_running_sum_greatest_revenue_units_percent_rank,
    cumulative_prev_running_avg_greatest_revenue_units,
    seq_no_str
FROM valid
