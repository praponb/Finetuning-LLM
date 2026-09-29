WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        SUBSTRING(channel, 1, 2) AS channel_prefix
    FROM promotions
    WHERE starts_on >= DATE '2023-03-30'
      AND channel IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        channel_prefix,
        ADD_MONTHS(starts_on, 1) AS starts_on_plus_months,
        LENGTH(channel) AS channel_length
    FROM valid
    WHERE channel IS NOT NULL
      AND starts_on > TO_DATE('2023-04-07')
),
with_window AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '7' DAY
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY promo_code ORDER BY discount_pct, id, promo_id) AS discount_pct_bucket,
        ROW_NUMBER() OVER (ORDER BY promo_id, id, promo_id) AS row_seq,
        AVG(promotion_discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS promotion_discount_pct_avg_2_prior
    FROM with_window
),
base AS (
    SELECT
        *,
        ROUND(promotion_discount_pct_avg_2_prior * 1.15, 2) AS promotion_discount_pct_avg_2_prior_adjusted,
        row_seq + discount_pct AS row_seq_plus_discount_pct,
        COALESCE(promotion_promo_code, 'UNKNOWN') AS promotion_promo_code_filled
    FROM with_windows
    WHERE starts_on > TO_DATE('2023-03-24')
      AND promotion_promo_code IS NOT DISTINCT FROM 'WELCOME5'
),
ranked AS (
    SELECT
        id,
        promo_code,
        discount_pct,
        starts_on_plus_months,
        channel_length,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct_avg_2_prior,
        promotion_discount_pct_avg_2_prior_adjusted,
        promotion_promo_code_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promo_id DESC, id, promo_id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    promo_code,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct_avg_2_prior,
    promotion_discount_pct_avg_2_prior_adjusted,
    promotion_promo_code_filled,
    CASE WHEN promotion_promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_A' WHEN promotion_promo_code RLIKE '^(SPRING10|VIP)$' THEN 'CORE' ELSE 'PRIORITY' END AS promotion_promo_code_zone
FROM ranked
ORDER BY promo_code;
