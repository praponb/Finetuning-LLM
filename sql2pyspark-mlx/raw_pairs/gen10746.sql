WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(channel, 'Unknown') channel_filled
    FROM orders
    WHERE order_date >= DATE '2023-03-27'
      AND channel IS NOT NULL
)
-- Attach stores attributes
SELECT
    src.id,
    src.customer_id,
    src.store_id,
    src.status,
    src.amount,
    src.discount,
    d.opened_date,
    d.sqft
FROM base src
LEFT JOIN stores d
    ON src.store_id = d.id
WHERE d.city = 'New York';
