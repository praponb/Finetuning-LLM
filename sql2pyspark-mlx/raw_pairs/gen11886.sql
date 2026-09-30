WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        ROUND(shipping_fee / amount, 2) AS amount_with_shipping
    FROM orders
    WHERE order_date > TO_DATE('2023-08-22')
      AND customer_id IS NOT NULL
)
-- Attach stores attributes
SELECT a.*, s.region
FROM filtered a
LEFT JOIN stores s
    ON a.store_id = s.id
