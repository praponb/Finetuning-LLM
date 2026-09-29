WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(country)) AS country_title,
        CONCAT(supplier_name, ', ', country) AS supplier_name_country
    FROM suppliers
    WHERE rating BETWEEN 2.8 AND 4.1
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost >= 320
        )
),
latest AS (
    SELECT
        id,
        supplier_name,
        city,
        country_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rank_in_group
        FROM not_flagged
    ) x
    WHERE rank_in_group = 1
),
latest_match AS (
    SELECT
        s.id,
        s.supplier_name,
        latest.brand,
        latest.category
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND SIZE(ch.tags) > 2
        )
),
kept_suppliers AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand != 'Stark'
        )
),
kept_suppliers_2 AS (
    SELECT s.*
    FROM kept_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price >= 950
        )
)
SELECT
    id,
    brand,
    CASE WHEN brand RLIKE '^(Initech|Umbrella)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS brand_zone
FROM kept_suppliers_2
ORDER BY brand NULLS LAST
