WITH parsed AS (
    SELECT
        id,
        created_at,
        items,
        attributes,
        attributes['device'] AS device,
        ship_to.city AS ship_city,
        TRANSFORM(
            FILTER(items, i -> i.promo IS NOT NULL),
            i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines
    FROM carts
)
SELECT
    ship_city,
    SORT_ARRAY(COLLECT_SET(ship_city)) AS ship_city_list,
    COUNT(CASE WHEN ship_city = 'New York' THEN 1 END) AS new_york_count,
    COUNT(DISTINCT device) AS distinct_devices
FROM parsed
GROUP BY ship_city
ORDER BY distinct_devices DESC NULLS FIRST;
