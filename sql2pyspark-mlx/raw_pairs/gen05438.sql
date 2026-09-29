WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        MAX(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount,
        SUM(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount,
        ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY discount DESC, id) AS row_seq
    FROM orders
    WHERE channel != 'web'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        running_sum_discount - running_max_discount AS running_sum_discount_minus_running_max_discount,
        CAST(running_count_discount AS STRING) AS running_count_discount_str,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM enriched
    WHERE order_date > TO_DATE('2023-02-20')
      AND channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(running_sum_discount) IGNORE NULLS OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_sum_discount,
        LEAD(amount, 2) OVER (ORDER BY discount, id) AS next_amount,
        AVG(amount) OVER (PARTITION BY DATE_TRUNC('week', order_date) ORDER BY running_sum_discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        SUM(running_count_discount) OVER (PARTITION BY channel ORDER BY running_max_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_discount,
        LAG(discount, 1, 0) OVER (PARTITION BY status ORDER BY discount, id) AS prev_discount
    FROM cleaned
),
enriched_orders AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        status,
        amount,
        discount,
        running_max_discount,
        running_sum_discount,
        row_seq,
        running_count_discount_str,
        channel_filled,
        last_known_running_sum_discount,
        next_amount,
        LAG(prev_discount) OVER (PARTITION BY channel, status ORDER BY shipping_fee, id) AS prev_prev_discount,
        LEAD(prev_discount) OVER (PARTITION BY channel, status ORDER BY shipping_fee, id) AS next_prev_discount,
        LAG(discount) OVER (PARTITION BY channel, status ORDER BY shipping_fee, id) AS prev_discount_2,
        ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY shipping_fee, id) AS event_seq
    FROM metrics
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM enriched_orders
    EXCEPT ALL
    SELECT status
    FROM orders
    WHERE amount IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_orders s
    INNER JOIN remaining_keys k ON s.status = k.status
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    status,
    kpi,
    val
FROM (
    SELECT
        status,
        running_max_discount,
        discount,
        amount
    FROM kept_rows
) src
UNPIVOT (
    val FOR kpi IN (running_max_discount, discount, amount AS AMOUNT_VAL)
)
