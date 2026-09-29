WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating NULLS FIRST, id) AS dedup_rank
        FROM suppliers
    ) ranked
    WHERE dedup_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT country, city
    FROM latest
    INTERSECT ALL
    SELECT country, city
    FROM suppliers
    WHERE rating < 3.1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.country = k.country AND s.city = k.city
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        dedup_rank,
        DENSE_RANK() OVER w_running AS rating_dense_rank,
        CUME_DIST() OVER (PARTITION BY city ORDER BY rating DESC) AS rating_cume_dist,
        PERCENT_RANK() OVER w_grp AS rating_percent_rank,
        DENSE_RANK() OVER (ORDER BY rating NULLS LAST) AS rating_dense_rank_2
    FROM kept_rows
    WINDOW w_running AS (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST), w_grp AS (ORDER BY rating DESC NULLS LAST)
),
scored_suppliers AS (
    -- Ranking and gap features
    SELECT
        rating,
        dedup_rank,
        rating_dense_rank_2,
        MAX(rating) OVER (PARTITION BY city, country) AS city_max_rating,
        COUNT(rating_percent_rank) OVER (PARTITION BY country) AS country_count_rating_percent_rank
    FROM scored
    WHERE rating_dense_rank BETWEEN 5 AND 8
)
-- Grouped metrics
SELECT
    CASE WHEN city_max_rating >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS city_max_rating_tier,
    APPROX_PERCENTILE(rating_dense_rank_2, 0.95) AS rating_dense_rank_2_p95,
    PERCENTILE_APPROX(rating_dense_rank_2, ARRAY(0.25, 0.5, 0.75)) AS rating_dense_rank_2_quartiles
FROM scored_suppliers
GROUP BY CASE WHEN city_max_rating >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING rating_dense_rank_2_p95 > 0
ORDER BY city_max_rating_tier DESC
