SELECT
    id,
    store_id,
    order_date,
    amount / NULLIF(shipping_fee, 0) AS amount_per_shipping_fee
FROM orders
