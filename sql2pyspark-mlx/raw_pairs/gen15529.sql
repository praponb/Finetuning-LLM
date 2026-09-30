WITH enriched AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        RANK() OVER (ORDER BY discount_pct) AS discount_pct_rank,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_discount_pct,
        RANK() OVER (ORDER BY discount_pct DESC) AS discount_pct_rank_2,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM promotions
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    next_discount_pct,
    discount_pct_rank_2,
    first_known_channel
FROM enriched
ORDER BY id DESC NULLS FIRST, starts_on DESC
LIMIT 20
