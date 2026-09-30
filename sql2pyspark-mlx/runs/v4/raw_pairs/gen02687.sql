WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE ends_on >= DATE '2024-01-04'
      AND channel IS NOT NULL
),
valid AS (
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM base
    WHERE ends_on > TO_DATE('2024-05-19')
      AND discount_pct > 0.14
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY ends_on ASC) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_title,
        channel_clean,
        channel_lower,
        resolved_channel,
        row_num,
        LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC) AS prev_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY row_num ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        LAST_VALUE(channel, TRUE) OVER w_hist AS last_known_channel
    FROM latest
    WINDOW w_hist AS (ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT *
FROM (
    SELECT
        id,
        channel,
        discount_pct,
        prev_discount_pct
    FROM scored
) src
UNPIVOT (
    metric_value FOR measure IN (discount_pct AS DISCOUNT_PCT_VAL, prev_discount_pct AS PREV_DISCOUNT_PCT)
);
