WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        channel,
        amount,
        discount,
        shipping_fee,
        CORR(shipping_fee, discount) OVER (PARTITION BY customer_id) AS corr_shipping_fee_discount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS row_seq,
        CORR(shipping_fee, amount) OVER (PARTITION BY store_id, channel) AS corr_shipping_fee_amount,
        LAG(discount, 1, 0) OVER w_recent AS prev_discount,
        LAG(shipping_fee, 1) OVER (ORDER BY amount, id) AS prev_shipping_fee
    FROM orders
    WINDOW w_recent AS (PARTITION BY MONTH(order_date), channel ORDER BY amount, id)
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        amount,
        shipping_fee,
        row_seq,
        prev_discount,
        prev_shipping_fee,
        AVG(shipping_fee) OVER w_grp AS shipping_fee_avg_2,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY customer_id, channel ORDER BY prev_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LEAD(corr_shipping_fee_amount, 2) OVER (PARTITION BY customer_id ORDER BY prev_shipping_fee, id) AS next_corr_shipping_fee_amount,
        SUM(row_seq) OVER (PARTITION BY channel ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        SUM(discount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY channel ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        amount,
        shipping_fee,
        prev_discount,
        prev_shipping_fee,
        shipping_fee_avg_2,
        first_known_amount,
        next_corr_shipping_fee_amount,
        cumulative_row_seq,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM with_windows_orders
    WHERE prev_shipping_fee > 10
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        prev_shipping_fee,
        next_corr_shipping_fee_amount,
        cumulative_row_seq,
        is_store,
        SUM(next_corr_shipping_fee_amount) OVER (PARTITION BY channel ORDER BY next_corr_shipping_fee_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_corr_shipping_fee_amount,
        AVG(prev_shipping_fee) OVER w_hist AS prev_shipping_fee_avg_2_prior
    FROM valid
    WHERE cumulative_row_seq BETWEEN 13 AND 14
    WINDOW w_hist AS (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        next_corr_shipping_fee_amount,
        cumulative_row_seq,
        prev_shipping_fee_avg_2_prior,
        LEAST(cumulative_next_corr_shipping_fee_amount, is_store) AS least_cumulative_next_corr_shipping_fee_amount_is_store
    FROM scored
    WHERE prev_shipping_fee > 11
)
SELECT
    id,
    next_corr_shipping_fee_amount,
    prev_shipping_fee_avg_2_prior,
    least_cumulative_next_corr_shipping_fee_amount_is_store
FROM filtered
ORDER BY prev_shipping_fee_avg_2_prior DESC;
