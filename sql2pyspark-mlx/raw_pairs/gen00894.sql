WITH base AS (
    SELECT
        *,
        DATE_SUB(order_date, 65) AS order_date_minus_65d,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE status IN ('CANCELLED', 'PENDING')
      AND order_date >= DATE '2023-06-18'
)
SELECT
    id,
    customer_id,
    channel,
    amount,
    CUME_DIST() OVER (ORDER BY discount NULLS LAST) AS discount_cume_dist,
    ROW_NUMBER() OVER (ORDER BY discount, id) AS row_seq
FROM base;
