WITH valid AS (
    SELECT *, QUARTER(ends_on) AS ends_on_quarter
    FROM promotions
    WHERE ends_on > DATE '2023-07-05'
      AND channel IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MAX(ends_on_quarter) OVER (PARTITION BY channel) AS channel_max_ends_on_quarter,
        AVG(ends_on_quarter) OVER (PARTITION BY channel ORDER BY ends_on_quarter, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ends_on_quarter_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on_quarter DESC, id) AS row_seq,
        FIRST_VALUE(ends_on_quarter) IGNORE NULLS OVER (PARTITION BY channel ORDER BY ends_on_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ends_on_quarter
    FROM valid
),
latest AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_quarter_avg_5_prior DESC NULLS FIRST, id) = 1
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '30' DAY
),
metrics_promotions AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        ends_on_quarter,
        ends_on_quarter_avg_5_prior,
        row_seq,
        first_known_ends_on_quarter,
        promo_id,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promotion_promo_code), 4) AS discount_pct_share,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct DESC, id, promo_id) AS event_seq
    FROM attributed
)
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    ends_on_quarter,
    ends_on_quarter_avg_5_prior,
    row_seq,
    promo_id,
    discount_pct_share
FROM metrics_promotions
ORDER BY ends_on_quarter NULLS FIRST
