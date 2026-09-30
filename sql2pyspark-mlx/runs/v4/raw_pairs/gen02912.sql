WITH base AS (
    SELECT *, SUBSTRING(status, 1, 4) AS status_prefix
    FROM orders
    WHERE channel IS NOT NULL
      AND status NOT IN ('COMPLETED', 'RETURNED')
)
SELECT *
FROM (
    SELECT
        id,
        channel,
        amount,
        shipping_fee,
        discount
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (amount, shipping_fee, discount)
);
