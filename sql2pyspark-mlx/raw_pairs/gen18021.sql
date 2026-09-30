WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct,
        NTILE(4) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM promotions
    WHERE MONTH(ends_on) IN (6, 8, 10)
)
SELECT
    id,
    ends_on,
    discount_pct,
    discount_pct_bucket,
    last_known_channel
FROM scored
ORDER BY id NULLS LAST, discount_pct DESC
LIMIT 20
