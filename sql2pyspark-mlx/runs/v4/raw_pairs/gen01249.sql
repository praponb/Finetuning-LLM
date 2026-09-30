WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS dedup_rank
        FROM promotions
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'GROUP_B' WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS promo_code_zone
FROM deduped
