WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(city) AS city_length,
        LENGTH(country) AS country_length,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE rating BETWEEN 2.1 AND 2.5
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM cleaned
    EXCEPT
    SELECT id, city
    FROM suppliers
    WHERE city IN ('London', 'Berlin', 'New York')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY rating DESC NULLS LAST) AS rating_cume_dist,
        DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank,
        SUM(rating) OVER w_grp AS country_sum_rating
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY country)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, country_sum_rating + rating_cume_dist AS country_sum_rating_plus_rating_cume_dist
    FROM scored
    WHERE city IS NOT NULL
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT city, id
    FROM base
    EXCEPT DISTINCT
    SELECT city, id
    FROM suppliers
    WHERE city != 'Singapore'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys_2 k ON s.city = k.city AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(rating_dense_rank / SUM(rating_dense_rank) OVER (PARTITION BY country), 4) AS rating_dense_rank_share,
        CUME_DIST() OVER w_ordered AS country_length_cume_dist
    FROM verified
    WINDOW w_ordered AS (PARTITION BY country ORDER BY country_length DESC)
),
scored_suppliers AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        MAX(city_length) OVER (PARTITION BY country, city) AS country_max_city_length
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    city,
    PERCENTILE_APPROX(rating_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS rating_dense_rank_quartiles,
    ROUND(SUM(rating_dense_rank_share) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS rating_dense_rank_share_per_chiang_mai
FROM scored_suppliers
GROUP BY CUBE (city)
ORDER BY city DESC NULLS LAST
