WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        coupon_codes,
        items,
        attributes,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS cart_value,
        SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
        TRANSFORM(
            FILTER(items, item -> item.promo IS NOT NULL),
            item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    coupon_codes,
    cart_value,
    NTILE(5) OVER (PARTITION BY customer_id ORDER BY cart_value DESC, id) AS cart_value_bucket,
    STDDEV_SAMP(bulk_lines) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_bulk_lines,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cart_value, id) AS seq_no,
    LAG(cart_value, 1) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id) AS prev_cart_value,
    LAG(cart_value, 1) OVER (PARTITION BY customer_id ORDER BY cart_value, id) AS prev_cart_value_2
FROM parsed;
