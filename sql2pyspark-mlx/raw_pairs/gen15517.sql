WITH metrics AS (
    SELECT
        *,
        NTILE(4) OVER w_part AS amount_bucket,
        RANK() OVER w_ordered AS shipping_fee_rank
    FROM orders
    WINDOW w_part AS (PARTITION BY MONTH(order_date), store_id ORDER BY amount, id), w_ordered AS (ORDER BY shipping_fee NULLS FIRST)
)
SELECT
    id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    amount_bucket,
    shipping_fee_rank
FROM metrics
ORDER BY discount DESC
