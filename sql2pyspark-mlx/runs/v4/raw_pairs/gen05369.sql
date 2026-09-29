WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_share
    FROM promotions
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_share
FROM with_windows
ORDER BY channel DESC;
