WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        MAX(sqft) OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_sqft,
        COUNT(sqft) OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_sqft,
        MIN(sqft) OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id ASC) AS seq_no
    FROM stores
)
-- Multi-level aggregation
SELECT
    region,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SIZE(COLLECT_SET(region)) AS num_regions,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50
FROM scored
GROUP BY region
HAVING sqft_p50 >= 0
ORDER BY sqft_p50 ASC NULLS LAST;
