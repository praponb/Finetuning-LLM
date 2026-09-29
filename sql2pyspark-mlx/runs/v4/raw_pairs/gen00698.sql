WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(city) AS clean_city,
        ROUND(rating * 1.15, 2) AS adjusted_rating,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        TRIM(city) AS clean_city_2,
        REPLACE(supplier_name, ' ', '-') AS supplier_name_slug,
        COALESCE(city, 'NONE') AS resolved_city
    FROM valid
    WHERE rating > 2.7
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        clean_city_2,
        supplier_name_slug,
        resolved_city,
        last_known_city,
        rating_avg_5,
        AVG(rating_avg_5) OVER (PARTITION BY country) AS country_avg_rating_avg_5,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no
    FROM with_windows
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rating_avg_5 >= 3 THEN 'LARGE' ELSE 'SMALL' END AS rating_avg_5_tier,
        seq_no,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
        ROUND(AVG(seq_no), 2) AS avg_seq_no
    FROM metrics
    GROUP BY CASE WHEN rating_avg_5 >= 3 THEN 'LARGE' ELSE 'SMALL' END, seq_no
    HAVING COUNT(*) > 1
)
SELECT
    rating_avg_5_tier,
    seq_no,
    num_rows,
    rating_quartiles,
    avg_seq_no
FROM daily
ORDER BY avg_seq_no DESC NULLS FIRST
