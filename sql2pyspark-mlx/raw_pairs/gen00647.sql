WITH unpacked AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attributes['device'] AS device,
        SIZE(items) AS num_items,
        SIZE(attributes) AS num_attributes
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
)
SELECT
    id,
    coupon_codes,
    device,
    num_items,
    num_attributes,
    STDDEV_SAMP(num_attributes) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_num_attributes,
    LEAD(num_items, 1) OVER (PARTITION BY customer_id ORDER BY num_items, id) AS next_num_items,
    AVG(num_attributes) OVER (ORDER BY num_attributes, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS num_attributes_avg_5,
    SUM(num_attributes) OVER (PARTITION BY customer_id ORDER BY num_attributes, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_num_attributes
FROM unpacked;
