WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        items,
        ship_to,
        attributes,
        attributes['utm_source'] AS utm_source,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS cart_value,
        attributes['gift'] AS gift,
        ship_to.city AS ship_city
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    MIN(created_at) AS first_created_at,
    SUM(CASE WHEN ship_city = 'Bangkok' THEN cart_value ELSE 0 END) AS bangkok_cart_value,
    MIN(created_at) AS first_seen
FROM flattened_attrs
GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at))
ORDER BY bangkok_cart_value DESC;
