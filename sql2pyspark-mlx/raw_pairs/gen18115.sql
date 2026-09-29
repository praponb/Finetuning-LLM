WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        discount / NULLIF(shipping_fee, 0) AS discount_per_shipping_fee,
        ROUND(amount * 1.15, 2) AS adjusted_amount
    FROM orders
    WHERE order_date > DATE '2023-04-08'
      AND channel IS DISTINCT FROM 'web'
)
SELECT store_id, channel
FROM filtered
EXCEPT
SELECT store_id, channel
FROM orders
WHERE customer_id IS NOT NULL
