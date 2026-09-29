WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN country IN ('JP', 'TH') THEN 'Core' ELSE 'Other' END AS country_group,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE supplier_name LIKE 'Tok%'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        rating,
        price,
        COUNT(rating) OVER (PARTITION BY city ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_rating
    FROM with_latest
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY last4_sum_rating NULLS FIRST, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 8
        )
)
SELECT
    id,
    country,
    city,
    last4_count_rating,
    dedup_rank,
    CASE WHEN country RLIKE '^(US|TH)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS country_zone
FROM kept;
