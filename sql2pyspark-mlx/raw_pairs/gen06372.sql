WITH filtered AS (
    SELECT
        id,
        promo_code,
        discount_pct,
        ROUND(discount_pct * 1.07, 2) AS discount_pct_adjusted,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on > DATE '2023-12-12'
      AND discount_pct BETWEEN 0.15 AND 0.32
)
SELECT
    id,
    promo_code,
    resolved_channel
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS rn
    FROM filtered
) t
WHERE rn = 1
