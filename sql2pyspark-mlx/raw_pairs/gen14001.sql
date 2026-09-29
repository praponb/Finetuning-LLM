WITH with_windows AS (
    SELECT
        *,
        SUM(sqft) OVER w_recent AS running_sum_sqft,
        COUNT(sqft) OVER w_recent AS running_count_sqft,
        MIN(sqft) OVER w_recent AS running_min_sqft,
        AVG(sqft) OVER w_recent AS running_avg_sqft,
        FIRST_VALUE(sqft) IGNORE NULLS OVER w_grp AS first_known_sqft
    FROM stores
    WHERE store_name LIKE '%Old%'
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN running_avg_sqft >= 15100 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_sqft_tier,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT_IF(region = 'South') AS south_events,
    APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90
FROM with_windows
GROUP BY CASE WHEN running_avg_sqft >= 15100 THEN 'LARGE' ELSE 'SMALL' END;
