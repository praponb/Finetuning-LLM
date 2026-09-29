WITH parsed AS (
    SELECT
        id,
        customer_id,
        items,
        ship_to,
        attributes,
        attributes['utm_source'] AS utm_source,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        attributes['device'] AS device,
        ship_to.city AS ship_city
    FROM carts
)
SELECT
    customer_id,
    SORT_ARRAY(COLLECT_SET(ship_city)) AS ship_city_list,
    SORT_ARRAY(COLLECT_SET(ship_city)) AS ship_city_list_2,
    COUNT_IF(ship_city = 'Singapore') AS singapore_events
FROM parsed
GROUP BY 1
ORDER BY customer_id;
