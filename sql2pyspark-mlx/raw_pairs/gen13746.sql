WITH remaining_keys AS (
    SELECT customer_id
    FROM orders
    EXCEPT ALL
    SELECT customer_id
    FROM orders
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM orders s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
base AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        COALESCE(channel, 'unassigned') channel_filled,
        COALESCE(channel, 'DIRECT') channel_filled_2
    FROM kept_rows
    WHERE channel IS DISTINCT FROM 'mobile'
),
scored AS (
    SELECT
        customer_id,
        order_date,
        status,
        channel,
        discount,
        channel_filled_2,
        CORR(discount, amount) OVER (PARTITION BY store_id) corr_discount_amount,
        DENSE_RANK() OVER w_part discount_dense_rank
    FROM base
    WINDOW w_part AS (PARTITION BY channel, customer_id ORDER BY discount DESC NULLS FIRST)
),
base_orders AS (
    SELECT
        customer_id,
        order_date,
        status,
        discount,
        channel_filled_2,
        corr_discount_amount,
        discount_dense_rank,
        corr_discount_amount + discount corr_discount_amount_plus_discount,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM scored
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        customer_id,
        corr_discount_amount_plus_discount,
        discount,
        corr_discount_amount
    FROM base_orders
) src
UNPIVOT (
    metric_value FOR metric_name IN (corr_discount_amount_plus_discount, discount AS DISCOUNT_VAL, corr_discount_amount AS CORR_DISCOUNT_AMOUNT_VAL)
)
