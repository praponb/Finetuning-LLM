WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        CUME_DIST() OVER (ORDER BY rating DESC NULLS LAST) AS rating_cume_dist
    FROM suppliers
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'eco')
        )
),
scored_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        LAG(rating_cume_dist, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating_cume_dist,
        AVG(cumulative_rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cumulative_rating_avg_5_prior,
        PERCENT_RANK() OVER (ORDER BY rating_cume_dist DESC) AS rating_cume_dist_percent_rank,
        LAST_VALUE(rating) IGNORE NULLS OVER w_recent AS last_known_rating,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) AS row_seq
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT city
    FROM scored_suppliers
    INTERSECT
    SELECT city
    FROM suppliers
    WHERE LOWER(TRIM(supplier_name)) = 'siam parts co'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_suppliers s
    INNER JOIN remaining_keys k ON s.city = k.city
)
SELECT
    id,
    city,
    rating,
    rating_cume_dist_percent_rank,
    last_known_rating,
    CASE WHEN city RLIKE '^(New York|Berlin)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS city_zone
FROM verified;
