WITH valid AS (
    SELECT
        *,
        DATE_FORMAT(ends_on, 'yyyy-MM') AS ends_on_label,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM promotions
    WHERE channel <> 'partner'
)
SELECT
    id,
    channel,
    starts_on,
    channel_clean,
    SUM(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
    ROW_NUMBER() OVER w_hist AS seq_no,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
FROM valid
WINDOW w_hist AS (PARTITION BY YEAR(starts_on), promo_code ORDER BY starts_on DESC, id);
