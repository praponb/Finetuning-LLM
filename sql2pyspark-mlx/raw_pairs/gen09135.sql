WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.4 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Chiang Mai'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN country = 'DE' THEN 1 ELSE 0 END AS is_de,
        TRIM(country) AS clean_country
    FROM cleaned
    WHERE city IS DISTINCT FROM 'Chiang Mai'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city IN ('Tokyo', 'Berlin') THEN 'Core' ELSE 'Other' END AS city_group,
        UPPER(country) AS country_upper,
        COALESCE(city, 'NONE') AS city_clean
    FROM valid
    WHERE supplier_name LIKE '%Gmb%'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        rating,
        is_de,
        city_group,
        MIN(rating) OVER (PARTITION BY country) AS country_min_rating,
        LAG(rating, 1) OVER (PARTITION BY city ORDER BY is_de, id) AS prev_rating
    FROM base
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= '2024-01-01'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        rating,
        is_de,
        LEAD(rating) OVER (PARTITION BY country ORDER BY country_min_rating, id) AS next_rating,
        LAG(country_min_rating) OVER (PARTITION BY country ORDER BY country_min_rating, id) AS prev_country_min_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY country_min_rating, id) AS event_seq
    FROM eligible
)
SELECT
    id,
    country,
    rating,
    CASE WHEN country RLIKE '^(DE|TH)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS country_zone
FROM metrics
ORDER BY id DESC, rating DESC
