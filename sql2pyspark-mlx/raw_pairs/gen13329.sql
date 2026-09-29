WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS rn
    FROM promotions
    QUALIFY rn = 1
)
SELECT
    id,
    channel,
    starts_on,
    discount_pct,
    rn
FROM deduped
ORDER BY discount_pct DESC NULLS LAST
