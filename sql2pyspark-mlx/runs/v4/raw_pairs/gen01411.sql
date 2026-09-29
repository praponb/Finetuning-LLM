SELECT
    id,
    customer_id,
    shipping_fee,
    RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS shipping_fee_rank
FROM orders
WHERE store_id IN (10, 36)
ORDER BY shipping_fee_rank DESC, 2 DESC
