WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(rating * 1.07, 2) AS rating_scaled,
        INITCAP(TRIM(supplier_name)) AS supplier_name_title
    FROM suppliers
    WHERE rating <= 3.4
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating_scaled DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating_scaled) OVER (PARTITION BY city) AS city_sum_rating_scaled,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY rating) AS rating_cume_dist,
        ROW_NUMBER() OVER (ORDER BY rating, id) AS row_seq,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY rating_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM ranked
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(rn * 1.15, 2) AS adjusted_rn
    FROM with_windows
    WHERE city IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rating_scaled >= 76 THEN 'LARGE' ELSE 'SMALL' END AS rating_scaled_tier,
    SUM(adjusted_rn) FILTER (WHERE city = 'Bangkok') AS bangkok_total_adjusted_rn,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(row_seq, 0.9) AS row_seq_p90,
    PERCENTILE_APPROX(city_sum_rating_scaled, 0.9) AS city_sum_rating_scaled_p90
FROM valid
GROUP BY CASE WHEN rating_scaled >= 76 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY row_seq_p90 NULLS LAST;
