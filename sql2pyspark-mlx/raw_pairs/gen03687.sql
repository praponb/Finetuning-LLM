WITH base AS (
    SELECT
        id,
        country,
        rating,
        SUBSTRING(supplier_name, 1, 3) supplier_name_prefix
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS NOT DISTINCT FROM 'Bangkok'
),
with_windows AS (
    SELECT
        *,
        SUM(rating) OVER w_running running_rating,
        NTILE(5) OVER (PARTITION BY country ORDER BY rating, id) rating_bucket
    FROM base
    WINDOW w_running AS (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price BETWEEN 600 AND 1130
        )
),
ranked AS (
    SELECT
        id,
        country,
        supplier_name_prefix
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_bucket DESC, id) = 1
),
latest_match AS (
    SELECT
        s.*,
        latest.category,
        latest.brand
    FROM ranked s
    JOIN LATERAL (
            SELECT c.category, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name_prefix,
    category,
    brand,
    CASE WHEN brand RLIKE '^(Initech|Acme)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS brand_zone
FROM latest_match
ORDER BY brand
