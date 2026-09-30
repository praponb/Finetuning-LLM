WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        rating,
        LAG(rating, 2) OVER w_grp AS prev_rating,
        FIRST_VALUE(rating, TRUE) OVER w_part AS first_known_rating
    FROM suppliers
    WINDOW w_grp AS (PARTITION BY country ORDER BY rating, id), w_part AS (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM enriched
    INTERSECT
    SELECT country
    FROM suppliers
    WHERE rating BETWEEN 3.0 AND 3.3
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.country = k.country
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (18, 23, 29, 36)
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country, id
    FROM not_flagged
    INTERSECT
    SELECT country, id
    FROM suppliers
    WHERE supplier_name LIKE '%Pacif%'
),
kept_rows_suppliers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN matched_keys k ON s.country = k.country AND s.id = k.id
)
-- Window metrics over several different partitions
SELECT
    id,
    country,
    ROUND(first_known_rating / SUM(first_known_rating) OVER (PARTITION BY country), 4) AS first_known_rating_share,
    RANK() OVER w_recent AS prev_rating_rank
FROM kept_rows_suppliers
WINDOW w_recent AS (PARTITION BY country ORDER BY prev_rating DESC NULLS FIRST)
