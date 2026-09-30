WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE revenue > 2600
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        revenue_filled,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        COUNT(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_revenue,
        LEAD(revenue_filled, 2) OVER (PARTITION BY product_id, store_id ORDER BY revenue_filled, id) AS next_revenue_filled
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(running_count_revenue) IGNORE NULLS OVER (ORDER BY next_revenue_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_revenue,
        FIRST_VALUE(running_count_revenue) IGNORE NULLS OVER (ORDER BY revenue_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_revenue_2,
        SUM(running_avg_revenue) OVER (ORDER BY running_avg_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_revenue
    FROM scored
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        sale_date,
        running_sum_revenue,
        running_avg_revenue,
        running_count_revenue,
        first_known_running_count_revenue,
        first_known_running_count_revenue_2,
        cumulative_running_avg_revenue,
        ROUND(cumulative_running_avg_revenue, 0) AS cumulative_running_avg_revenue_rounded,
        COALESCE(cumulative_running_avg_revenue, 0) AS cumulative_running_avg_revenue_filled
    FROM enriched
    WHERE sale_date > DATE '2023-02-27'
)
-- Grouped metrics
SELECT
    CASE WHEN running_sum_revenue >= 15200 THEN 'HIGH' ELSE 'LOW' END AS running_sum_revenue_tier,
    PERCENTILE_APPROX(cumulative_running_avg_revenue, ARRAY(0.25, 0.5, 0.75)) AS cumulative_running_avg_revenue_quartiles,
    PERCENTILE_APPROX(first_known_running_count_revenue, ARRAY(0.25, 0.5, 0.75)) AS first_known_running_count_revenue_quartiles
FROM filtered
GROUP BY 1
ORDER BY running_sum_revenue_tier DESC
