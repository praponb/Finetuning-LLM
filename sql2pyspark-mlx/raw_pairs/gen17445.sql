WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(DATEDIFF(starts_on, ends_on) / 30, 1) AS months_ends_on_to_starts_on,
        LAST_DAY(starts_on) AS starts_on_month_end,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2023-09-16'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY starts_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        PERCENT_RANK() OVER (ORDER BY months_ends_on_to_starts_on NULLS LAST) AS months_ends_on_to_starts_on_percent_rank,
        ROW_NUMBER() OVER (ORDER BY starts_on_month_end, id) AS row_seq
    FROM base
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 7)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        months_ends_on_to_starts_on,
        starts_on_month_end,
        months_ends_on_to_starts_on_percent_rank,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        TRIM(promo_code) AS clean_promo_code,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM with_window
    WHERE ends_on >= DATE '2023-10-07'
      AND promo_id < 8
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        months_ends_on_to_starts_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        clean_promo_code,
        channel_clean,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key,
        COALESCE(promotion_promo_code, 'NONE') AS promotion_promo_code_clean
    FROM filtered
    WHERE ends_on > DATE '2023-08-04'
      AND channel IS NOT DISTINCT FROM 'store'
)
-- Grouped metrics
SELECT
    promo_id,
    SUM(promo_id) FILTER (WHERE channel = 'partner') AS partner_total_promo_id,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'WELCOME5') AS welcome5_rows,
    APPROX_PERCENTILE(promotion_discount_pct, 0.5) AS promotion_discount_pct_p50
FROM cleaned
GROUP BY promo_id;
