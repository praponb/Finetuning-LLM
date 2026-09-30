WITH ranked AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY starts_on DESC) AS dedup_rank
        FROM promotions
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    CASE WHEN promo_code RLIKE '^(WELCOME5|SPRING10)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS promo_code_zone
FROM ranked
ORDER BY discount_pct DESC NULLS FIRST;
