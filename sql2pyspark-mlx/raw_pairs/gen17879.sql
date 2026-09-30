SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    MIN(discount_pct) OVER w_grp AS ends_on_month_min_discount_pct,
    FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    NTILE(10) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id ASC) AS discount_pct_bucket,
    CUME_DIST() OVER w_hist AS discount_pct_cume_dist,
    RANK() OVER (ORDER BY discount_pct ASC) AS discount_pct_rank
FROM promotions
WINDOW w_grp AS (PARTITION BY MONTH(ends_on), promo_code), w_hist AS (PARTITION BY promo_code ORDER BY discount_pct DESC)
