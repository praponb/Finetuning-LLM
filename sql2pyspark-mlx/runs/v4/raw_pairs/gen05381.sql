WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS event_seq
    FROM stores
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(AVG(sqft_avg_2), 2) AS avg_sqft_avg_2,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    PERCENTILE_APPROX(sqft, 0.95) AS sqft_p95
FROM enriched
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('WEEK', opened_date)), (DATE_TRUNC('YEAR', opened_date)), ())
ORDER BY year_start NULLS LAST;
