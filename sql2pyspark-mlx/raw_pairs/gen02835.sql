WITH base AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) days_since_sale_date,
        ROUND(units, 2) units_rounded
    FROM store_sales
    WHERE units BETWEEN 20 AND 32
),
latest AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id ASC) = 1
),
cleaned AS (
    SELECT *, days_since_sale_date - units_rounded days_since_sale_date_minus_units_rounded
    FROM latest
    WHERE sale_date >= DATE '2023-02-21'
),
scored AS (
    SELECT
        id,
        product_id,
        sale_date,
        units_rounded,
        COUNT(revenue) OVER w_grp running_count_revenue,
        AVG(revenue) OVER w_grp running_avg_revenue,
        MIN(revenue) OVER w_grp running_min_revenue,
        MAX(revenue) OVER w_grp running_max_revenue
    FROM cleaned
    WHERE days_since_sale_date = 46
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY units_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        id,
        units_rounded,
        running_min_revenue,
        MIN(running_avg_revenue) OVER w_recent last7_min_running_avg_revenue,
        MAX(running_avg_revenue) OVER w_recent last7_max_running_avg_revenue,
        SUM(running_avg_revenue) OVER w_recent last7_sum_running_avg_revenue,
        COUNT(running_avg_revenue) OVER w_recent last7_count_running_avg_revenue,
        LEAD(running_avg_revenue, 2) OVER (ORDER BY running_min_revenue ASC, id ASC) next_running_avg_revenue
    FROM scored
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY units_rounded ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored_store_sales AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY units_rounded DESC) units_rounded_cume_dist,
        LAG(next_running_avg_revenue, 2) OVER (ORDER BY last7_count_running_avg_revenue ASC, id ASC) prev_next_running_avg_revenue
    FROM metrics
)
SELECT
    CASE WHEN next_running_avg_revenue >= 4400 THEN 'HIGH' ELSE 'LOW' END AS next_running_avg_revenue_tier,
    COUNT(*) AS event_count,
    AVG(last7_min_running_avg_revenue) AS avg_last7_min_running_avg_revenue,
    ROUND(AVG(units_rounded_cume_dist), 2) AS avg_units_rounded_cume_dist
FROM scored_store_sales
GROUP BY CASE WHEN next_running_avg_revenue >= 4400 THEN 'HIGH' ELSE 'LOW' END
ORDER BY next_running_avg_revenue_tier DESC NULLS LAST
