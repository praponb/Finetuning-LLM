WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        CAST(amount AS INT) AS amount_int,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE order_date > DATE '2023-07-04'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        channel,
        discount,
        shipping_fee,
        amount_int,
        SUM(discount) OVER w_grp AS running_discount,
        LEAD(discount, 1) OVER w_part AS next_discount
    FROM filtered
    WHERE store_id IS NULL
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY amount_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY channel, store_id ORDER BY amount_int, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_discount,
        MAX(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_discount
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        channel,
        discount,
        shipping_fee,
        amount_int,
        last3_count_discount,
        MAX(running_discount) OVER w_part AS running_max_running_discount,
        SUM(running_discount) OVER w_part AS running_sum_running_discount,
        COUNT(running_discount) OVER w_part AS running_count_running_discount,
        AVG(running_discount) OVER w_part AS running_avg_running_discount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount_int DESC, id) AS row_seq
    FROM enriched
    WINDOW w_part AS (PARTITION BY channel ORDER BY last3_sum_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(row_seq) OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee, id) AS next_row_seq,
        LEAD(shipping_fee) OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee, id) AS next_shipping_fee,
        LAG(shipping_fee) OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee, id) AS seq_no,
        CORR(discount, shipping_fee) OVER (PARTITION BY customer_id, channel) AS corr_discount_shipping_fee
    FROM with_windows
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        channel,
        discount,
        amount_int,
        last3_count_discount,
        running_max_running_discount,
        running_sum_running_discount,
        next_shipping_fee,
        prev_shipping_fee,
        corr_discount_shipping_fee
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_row_seq DESC, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        channel,
        amount_int
    FROM latest
) src
PIVOT (
    MAX(amount_int) AS total
    FOR channel IN ('store', 'partner')
)
