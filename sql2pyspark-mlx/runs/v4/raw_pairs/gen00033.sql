WITH parsed AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        ship_to,
        attributes['gift'] AS gift,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price
    FROM carts
)
SELECT
    *,
    NTILE(10) OVER (PARTITION BY customer_id ORDER BY max_item_price, id) AS max_item_price_bucket,
    FIRST_VALUE(max_item_price) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_item_price,
    STDDEV_SAMP(max_item_price) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_max_item_price,
    NTILE(10) OVER (PARTITION BY customer_id ORDER BY max_item_price, id) AS max_item_price_bucket_2
FROM parsed
