WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CASE WHEN amount >= 1390 THEN 'High' WHEN amount >= 390 THEN 'Medium' ELSE 'Low' END AS amount_level,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM orders
    WHERE order_date > TO_DATE('2023-03-28')
      AND channel IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    status,
    kpi,
    metric_value
FROM (
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        amount
    FROM valid
) src
UNPIVOT (
    metric_value FOR kpi IN (discount AS DISCOUNT, shipping_fee AS SHIPPING_FEE_VAL, amount)
) long_rows;
