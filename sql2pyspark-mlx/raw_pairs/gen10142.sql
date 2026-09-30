WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        city,
        rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        SUM(rating) OVER w_running AS running_rating_2,
        SUM(rating) OVER w_running AS running_rating_3
    FROM suppliers
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating >= 4.1 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    CASE WHEN running_rating_2 >= 17 THEN 'LARGE' ELSE 'SMALL' END AS running_rating_2_tier,
    PERCENTILE_APPROX(running_rating, ARRAY(0.25, 0.5, 0.75)) AS running_rating_quartiles,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Tokyo' THEN running_rating_2 ELSE 0 END) AS tokyo_running_rating_2,
    APPROX_PERCENTILE(running_rating_3, 0.9) AS running_rating_3_p90,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN rating >= 4.1 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(city)) DESC) AS num_cities_percent_rank
FROM with_windows
GROUP BY CASE WHEN rating >= 4.1 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_rating_2 >= 17 THEN 'LARGE' ELSE 'SMALL' END;
