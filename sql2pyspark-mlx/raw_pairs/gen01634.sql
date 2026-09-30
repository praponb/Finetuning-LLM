WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE ends_on > TO_DATE('2023-09-22')
      AND ends_on > '2025-07-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        resolved_channel,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS event_seq,
        SUM(discount_pct) OVER (PARTITION BY DATE_TRUNC('YEAR', starts_on), channel) AS starts_on_period_sum_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM base
    WHERE YEAR(starts_on) >= 2025
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY channel ORDER BY starts_on_period_sum_discount_pct) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY channel ORDER BY starts_on_period_sum_discount_pct DESC, id) AS starts_on_period_sum_discount_pct_bucket,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS discount_pct_bucket,
        LAST_VALUE(cumulative_discount_pct) IGNORE NULLS OVER (PARTITION BY channel, promo_code ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_discount_pct
    FROM latest
),
scored_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        event_seq,
        starts_on_period_sum_discount_pct,
        cumulative_discount_pct,
        row_num,
        starts_on_period_sum_discount_pct_bucket,
        discount_pct_bucket,
        last_known_cumulative_discount_pct,
        LEAD(starts_on_period_sum_discount_pct_bucket) OVER w_recent AS next_starts_on_period_sum_discount_pct_bucket,
        LAG(last_known_cumulative_discount_pct) OVER w_recent AS prev_last_known_cumulative_discount_pct,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY row_num, id)
)
SELECT
    id,
    starts_on_period_sum_discount_pct,
    cumulative_discount_pct,
    starts_on_period_sum_discount_pct_bucket,
    discount_pct_bucket,
    last_known_cumulative_discount_pct,
    next_starts_on_period_sum_discount_pct_bucket,
    prev_last_known_cumulative_discount_pct
FROM scored_promotions
ORDER BY discount_pct_bucket DESC;
