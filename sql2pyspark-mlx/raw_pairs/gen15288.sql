WITH filtered AS (
    SELECT
        *,
        LENGTH(promo_code) promo_code_length,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND discount_pct < 0.2
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) = 1;
