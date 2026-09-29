WITH lines AS (
    SELECT
        t.id,
        t.customer_id,
        line.product_id,
        line.qty,
        line.price
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) li AS line
),
joined AS (
    SELECT
        src.id,
        src.customer_id,
        src.qty,
        d.name,
        d.signup_date,
        d.country
    FROM lines src
    JOIN customers d
        ON src.customer_id <=> d.id
),
enriched AS (
    SELECT
        s.customer_id,
        s.qty,
        s.name,
        s.country,
        d.email
    FROM joined s
    JOIN customers d
        ON s.customer_id <=> d.id
),
with_dims AS (
    SELECT
        a.customer_id,
        a.name,
        a.email,
        p.city
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
)
SELECT
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(CASE WHEN city = 'Berlin' THEN 1 END) AS berlin_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2
FROM with_dims
GROUP BY city
