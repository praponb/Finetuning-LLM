WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, b.country
    FROM exploded a
    INNER JOIN customers b
        ON a.customer_id <=> b.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(country, 'unassigned') country_filled,
        CONCAT_WS(',', tag, country) tag_country_key
    FROM with_dims
    WHERE tag IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.tag,
        src.country,
        src.tag_country_key,
        p.city
    FROM filtered src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    country,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
FROM joined
GROUP BY city, country
ORDER BY distinct_customer_ids
