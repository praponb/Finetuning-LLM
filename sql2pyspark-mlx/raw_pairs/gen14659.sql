WITH kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM suppliers
    EXCEPT ALL
    SELECT city
    FROM suppliers
    WHERE rating <= 4.1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN kept_keys k ON s.city = k.city
)
-- Aggregate with conditional metrics
SELECT
    country,
    CASE WHEN rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) DESC) AS jp_rating_dense_rank
FROM kept_rows
GROUP BY country, CASE WHEN rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END;
