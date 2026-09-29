WITH scored AS (
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY YEAR(ends_on)) AS ends_on_year_count_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on, id) AS row_seq
    FROM promotions
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    ends_on_year_count_discount_pct,
    row_seq,
    CASE WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'OTHER_ZONE' WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS promo_code_zone
FROM scored
ORDER BY starts_on
