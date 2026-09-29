WITH remaining_keys AS (
    -- Set difference on keys
    SELECT country
    FROM suppliers
    EXCEPT
    SELECT country
    FROM suppliers
    WHERE city NOT IN ('Tokyo', 'New York', 'London')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN remaining_keys k ON s.country = k.country
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY rating) AS rating_cume_dist,
        RANK() OVER (ORDER BY rating DESC) AS rating_rank,
        SUM(rating) OVER w_hist AS country_sum_rating
    FROM verified
    WINDOW w_hist AS (PARTITION BY country)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date > '2023-09-05'
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        country_sum_rating
    FROM not_flagged
) src
PIVOT (
    AVG(country_sum_rating)
    FOR country IN ('TH', 'JP')
);
