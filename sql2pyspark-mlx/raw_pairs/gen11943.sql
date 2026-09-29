SELECT
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    order_id,
    BIT_AND(quantity) AS quantity_bit_and,
    STDDEV_SAMP(unit_price) AS unit_price_stddev
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END, order_id
