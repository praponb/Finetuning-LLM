WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE city IN ('Bangkok', 'Tokyo')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM eligible
    WHERE region != 'Central'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft) OVER w_running AS next_sqft,
        LAG(sqft) OVER w_running AS prev_sqft,
        LAG(cumulative_sqft) OVER w_running AS prev_cumulative_sqft,
        LEAD(cumulative_sqft) OVER w_running AS next_cumulative_sqft,
        ROW_NUMBER() OVER w_running AS seq_no,
        MIN(cumulative_sqft) OVER (PARTITION BY YEAR(opened_date)) AS opened_date_year_min_cumulative_sqft
    FROM enriched
    WINDOW w_running AS (PARTITION BY city ORDER BY cumulative_sqft, id)
)
-- Multi-level aggregation
SELECT
    region,
    CASE WHEN prev_cumulative_sqft >= 60727 THEN 'LARGE' ELSE 'SMALL' END AS prev_cumulative_sqft_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    AVG(next_sqft) AS avg_next_sqft,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM scored
GROUP BY region, CASE WHEN prev_cumulative_sqft >= 60727 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', opened_date)
ORDER BY prev_cumulative_sqft_tier DESC NULLS FIRST;
