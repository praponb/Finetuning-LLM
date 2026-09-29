WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on, id) = 1
)
SELECT
    id,
    channel,
    starts_on,
    discount_pct
FROM deduped
ORDER BY channel NULLS LAST, id
