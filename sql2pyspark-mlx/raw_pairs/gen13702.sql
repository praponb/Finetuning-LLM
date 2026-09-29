WITH base AS (
    SELECT
        id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) / 365.25 AS years_since_order_date,
        ADD_MONTHS(order_date, 3) AS order_date_plus_months
    FROM orders
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'mobile'
)
SELECT
    id,
    order_date,
    amount,
    shipping_fee,
    order_date_plus_months,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY years_since_order_date DESC) AS years_since_order_date_percent_rank,
    DENSE_RANK() OVER (ORDER BY years_since_order_date DESC NULLS FIRST) AS years_since_order_date_dense_rank,
    CORR(shipping_fee, amount) OVER (PARTITION BY status) AS corr_shipping_fee_amount
FROM base
