WITH with_windows AS (
    SELECT
        *,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_avg_3
    FROM store_sales
)
SELECT
    store_id,
    CASE WHEN units_avg_3 >= 32 THEN 'HIGH' ELSE 'LOW' END AS units_avg_3_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(units) AS units_stddev,
    COUNT(*) AS num_rows_2,
    AVG(corr_units_revenue) AS avg_corr_units_revenue,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY GROUPING SETS ((store_id, CASE WHEN units_avg_3 >= 32 THEN 'HIGH' ELSE 'LOW' END), (store_id))
ORDER BY cnt;
