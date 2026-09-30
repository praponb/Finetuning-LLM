SELECT
    id,
    channel,
    COALESCE(shipping_fee, 0) AS shipping_fee_filled,
    DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
FROM orders
WHERE shipping_fee <= 38;
