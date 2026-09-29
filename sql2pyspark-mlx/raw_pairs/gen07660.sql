WITH enriched AS (
    SELECT
        id,
        starts_on,
        ends_on,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code, channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        AVG(discount_pct) OVER w_part AS discount_pct_avg_5,
        LEAD(discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS next_discount_pct,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS next_discount_pct_2,
        RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_rank
    FROM promotions
    WHERE YEAR(starts_on) >= 2025
    WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    ends_on,
    last_known_channel,
    discount_pct_avg_5,
    next_discount_pct,
    next_discount_pct_2,
    discount_pct_rank
FROM enriched
ORDER BY id DESC NULLS FIRST;
