WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, v
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.created_at,
        a.attr_key,
        a.v,
        d.email,
        d.age,
        d.city
    FROM exploded a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(age) OVER (PARTITION BY city) AS city_min_age,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY age NULLS FIRST) AS age_cume_dist,
        COUNT(age) OVER (PARTITION BY TO_DATE(created_at), attr_key) AS created_at_to_date_count_age,
        ROUND(age / SUM(age) OVER (PARTITION BY attr_key), 4) AS age_share,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY age DESC NULLS FIRST) AS age_percent_rank
    FROM with_dims
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        v,
        age,
        city,
        city_min_age,
        age_cume_dist,
        age_share,
        CASE WHEN age >= 71 THEN 'High' WHEN age >= 27 THEN 'Medium' ELSE 'Low' END AS age_band,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM metrics
    WHERE age_percent_rank <= 0.64
),
grouped AS (
    -- Grouped metrics
    SELECT
        city,
        CASE WHEN age_cume_dist >= 0.83 THEN 'HIGH' ELSE 'LOW' END AS age_cume_dist_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        ROUND(AVG(city_min_age), 2) AS avg_city_min_age,
        AVG(age) FILTER (WHERE city = 'London') AS london_avg_age,
        ROUND(AVG(city_min_age), 2) AS avg_city_min_age_2,
        COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count
    FROM valid
    GROUP BY GROUPING SETS ((city, CASE WHEN age_cume_dist >= 0.83 THEN 'HIGH' ELSE 'LOW' END), (city), ())
    HAVING COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) > 1
)
SELECT
    distinct_ids,
    avg_city_min_age,
    london_avg_age,
    avg_city_min_age_2
FROM grouped;
