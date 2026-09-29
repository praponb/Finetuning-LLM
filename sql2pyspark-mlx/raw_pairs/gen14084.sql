WITH with_windows AS (
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        NTILE(4) OVER w_hist AS discount_pct_bucket,
        LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC) AS prev_discount_pct
    FROM promotions
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct ASC, id ASC)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_avg_6_prior,
    discount_pct_bucket,
    prev_discount_pct
FROM with_windows
ORDER BY id DESC NULLS FIRST
