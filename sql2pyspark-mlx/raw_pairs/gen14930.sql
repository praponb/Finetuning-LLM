WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        CONCAT(city, ' ', store_name) AS city_store_name,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-08-26'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date) ORDER BY sqft, id) AS seq_no,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        DENSE_RANK() OVER (ORDER BY sqft DESC) AS sqft_dense_rank
    FROM valid
    WHERE city = 'Bangkok'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.product_id IN (2, 4, 7)
        )
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.store_id IN (2, 4, 5, 6)
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(city) AS clean_city
    FROM eligible_stores
    WHERE sqft_avg_2 > 15802
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft_avg_2 >= 13575 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_avg_2_tier,
        MAX(seq_no) AS max_seq_no,
        AVG(cumulative_sqft) FILTER (WHERE city = 'Bangkok') AS bangkok_avg_cumulative_sqft,
        AVG(sqft_dense_rank) FILTER (WHERE city = 'Tokyo') AS tokyo_avg_sqft_dense_rank,
        APPROX_PERCENTILE(seq_no, 0.9) AS seq_no_p90
    FROM filtered
    GROUP BY sqft_avg_2_tier
)
SELECT
    sqft_avg_2_tier,
    max_seq_no,
    bangkok_avg_cumulative_sqft
FROM summary
ORDER BY bangkok_avg_cumulative_sqft DESC;
