WITH enriched AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CORR(discount, amount) OVER (PARTITION BY channel, status) AS corr_discount_amount,
        LAG(amount, 1) OVER (ORDER BY amount, id) AS prev_amount,
        AVG(shipping_fee) OVER (ORDER BY shipping_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_5,
        RANK() OVER (PARTITION BY MONTH(order_date), channel ORDER BY amount DESC) AS amount_rank,
        AVG(shipping_fee) OVER (PARTITION BY YEAR(order_date), channel ORDER BY shipping_fee, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_2_prior
    FROM orders
),
filtered AS (
    SELECT
        id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        corr_discount_amount,
        prev_amount,
        amount_rank,
        shipping_fee_avg_2_prior,
        COALESCE(shipping_fee, 0) AS shipping_fee_filled,
        CAST(shipping_fee_avg_5 AS STRING) AS shipping_fee_avg_5_str
    FROM enriched
    WHERE order_date > DATE '2023-03-24'
      AND customer_id IN (5, 15, 28, 30)
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.carrier = 'Flash'
        )
),
scored AS (
    SELECT
        id,
        status,
        shipping_fee,
        corr_discount_amount,
        prev_amount,
        amount_rank,
        shipping_fee_filled,
        shipping_fee_avg_5_str,
        COUNT(prev_amount) OVER w_hist AS last4_count_prev_amount,
        AVG(prev_amount) OVER w_hist AS last4_avg_prev_amount,
        MIN(prev_amount) OVER w_hist AS last4_min_prev_amount,
        LAST_VALUE(prev_amount, TRUE) OVER (PARTITION BY status ORDER BY shipping_fee_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_amount
    FROM not_flagged
    WHERE prev_amount > 920
    WINDOW w_hist AS (PARTITION BY channel, status ORDER BY corr_discount_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (14, 27, 31)
        )
)
SELECT
    id,
    status,
    measure,
    val
FROM (
    SELECT
        id,
        status,
        prev_amount,
        last4_avg_prev_amount
    FROM kept
) src
UNPIVOT (
    val FOR measure IN (prev_amount, last4_avg_prev_amount)
) long_rows;
