SELECT
    id,
    customer_id,
    shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS shipping_fee_row
FROM orders;
