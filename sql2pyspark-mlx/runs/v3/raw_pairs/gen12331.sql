SELECT
    id,
    customer_id,
    shipping_fee,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS shipping_fee_pct_rank
FROM orders
WHERE store_id IS NULL;
