WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        QUARTER(sale_date) AS sale_date_quarter
    FROM store_sales
    WHERE product_id IN (4, 5)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sale_date_quarter,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        SUM(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units_2
    FROM cleaned
),
joined AS (
    -- Attach products attributes
    SELECT src.*, d.product_name
    FROM metrics src
    LEFT JOIN products d
        ON src.product_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sale_date_quarter) OVER w_grp AS running_sum_sale_date_quarter,
        AVG(sale_date_quarter) OVER w_grp AS running_avg_sale_date_quarter,
        MIN(sale_date_quarter) OVER w_grp AS running_min_sale_date_quarter,
        COUNT(sale_date_quarter) OVER w_grp AS running_count_sale_date_quarter,
        SUM(cumulative_units_2) OVER (PARTITION BY product_id ORDER BY sale_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_units_2
    FROM joined
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY cumulative_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        sale_date_quarter,
        cumulative_units,
        cumulative_cumulative_units_2,
        LAG(running_count_sale_date_quarter) OVER (PARTITION BY product_id ORDER BY running_min_sale_date_quarter, id) AS prev_running_count_sale_date_quarter,
        LAG(running_min_sale_date_quarter) OVER (PARTITION BY product_id ORDER BY running_min_sale_date_quarter, id) AS prev_running_min_sale_date_quarter,
        LEAD(running_min_sale_date_quarter) OVER (PARTITION BY product_id ORDER BY running_min_sale_date_quarter, id) AS next_running_min_sale_date_quarter,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_min_sale_date_quarter, id) AS row_seq,
        AVG(cumulative_cumulative_units_2) OVER (PARTITION BY product_id) AS product_id_avg_cumulative_cumulative_units_2
    FROM scored
    WHERE cumulative_units < 157
)
-- Multi-level aggregation
SELECT
    CASE WHEN product_id_avg_cumulative_cumulative_units_2 >= 1020 THEN 'HIGH' ELSE 'LOW' END AS product_id_avg_cumulative_cumulative_units_2_tier,
    CASE WHEN cumulative_cumulative_units_2 >= 921 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_cumulative_units_2_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(prev_running_count_sale_date_quarter, ARRAY(0.25, 0.5, 0.75)) AS prev_running_count_sale_date_quarter_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN product_id_avg_cumulative_cumulative_units_2 >= 1020 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM with_windows
GROUP BY CASE WHEN product_id_avg_cumulative_cumulative_units_2 >= 1020 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cumulative_cumulative_units_2 >= 921 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT id) > 1
ORDER BY product_id_avg_cumulative_cumulative_units_2_tier NULLS FIRST
