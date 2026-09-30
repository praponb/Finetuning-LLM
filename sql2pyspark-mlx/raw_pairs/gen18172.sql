WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CONCAT_WS('-', channel, status) AS channel_status_key,
        DATEDIFF(CURRENT_DATE(), order_date) / 365.25 AS years_since_order_date
    FROM orders
    WHERE customer_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        discount,
        amount
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (discount, amount)
);
