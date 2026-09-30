WITH with_windows AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        COUNT(revenue) OVER w_part AS running_count_revenue,
        MAX(revenue) OVER w_part AS running_max_revenue,
        SUM(revenue) OVER w_part AS running_sum_revenue
    FROM store_sales
    WINDOW w_part AS (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN running_count_revenue >= 18 THEN 'LARGE' ELSE 'SMALL' END AS running_count_revenue_tier,
    ROUND(AVG(running_count_revenue), 2) AS avg_running_count_revenue,
    ROUND(AVG(units), 2) AS avg_units,
    APPROX_PERCENTILE(running_count_revenue, 0.9) AS running_count_revenue_p90
FROM with_windows
GROUP BY CASE WHEN running_count_revenue >= 18 THEN 'LARGE' ELSE 'SMALL' END;
