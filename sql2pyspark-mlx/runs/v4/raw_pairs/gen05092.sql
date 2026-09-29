WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        attributes,
        TRANSFORM(
            FILTER(items, li -> li.promo IS NOT NULL),
            li -> STRUCT(li.product_id AS pid, ROUND(li.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        FORALL(items, li -> li.qty <= 5) AS all_small_qty,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        attributes['utm_source'] AS utm_source
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    MIN(created_at) AS first_seen,
    COUNT(*) AS row_count,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    COUNT(*) AS num_rows,
    MIN(created_at) AS first_seen_2
FROM flattened_attrs
GROUP BY DATE_TRUNC('HOUR', created_at), DATE_TRUNC('DAY', created_at);
