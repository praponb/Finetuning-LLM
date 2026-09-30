WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        DATE_ADD(order_date, 27) order_date_plus_27d,
        amount / NULLIF(discount, 0) amount_per_discount
    FROM orders
    WHERE order_date >= DATE '2023-08-30'
      AND channel IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    amount,
    discount,
    order_date_plus_27d
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY discount) AS rank_in_group
    FROM cleaned
) x
WHERE rank_in_group = 1;
