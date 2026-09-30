WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN region = 'North' THEN 1 ELSE 0 END AS is_north
    FROM stores
    WHERE YEAR(opened_date) >= 2023
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft) OVER (ORDER BY is_north, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY YEAR(opened_date), city), 4) AS sqft_share
    FROM filtered
    WHERE region <> 'South'
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, region
    FROM with_windows
    EXCEPT DISTINCT
    SELECT city, region
    FROM stores
    WHERE opened_date < '2024-10-21'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.city = k.city AND s.region = k.region
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        is_north,
        cumulative_sqft,
        sqft_share,
        COALESCE(cumulative_sqft, 0) AS cumulative_sqft_filled,
        COALESCE(region, 'NONE') AS region_filled
    FROM reconciled
    WHERE city IN ('Tokyo', 'Singapore')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(cumulative_sqft) OVER w_ordered AS city_max_cumulative_sqft,
        SUM(sqft) OVER w_ordered AS city_sum_sqft,
        AVG(is_north) OVER w_ordered AS city_avg_is_north,
        MIN(is_north) OVER w_part AS opened_date_period_min_is_north
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY city), w_part AS (PARTITION BY DATE_TRUNC('YEAR', opened_date), city)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_sqft >= 72306 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_sqft_tier,
    city,
    APPROX_PERCENTILE(is_north, 0.5) AS is_north_p50,
    APPROX_PERCENTILE(city_avg_is_north, ARRAY(0.25, 0.5, 0.75)) AS city_avg_is_north_quartiles
FROM enriched
GROUP BY CASE WHEN cumulative_sqft >= 72306 THEN 'LARGE' ELSE 'SMALL' END, city
ORDER BY cumulative_sqft_tier DESC;
