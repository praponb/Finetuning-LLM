WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN brand IN ('Initech', 'Acme') THEN 'Core' ELSE 'Other' END AS brand_group,
        QUARTER(launch_date) AS launch_date_quarter,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
      AND YEAR(launch_date) >= 2025
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_quantity
    FROM filtered s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.quantity >= 175
        )
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.price,
        s.cost,
        s.tags,
        s.supplier_id,
        s.launch_date_quarter,
        s.brand_clean,
        s.inventory_quantity,
        p.city
    FROM eligible s
    JOIN suppliers p
        ON s.supplier_id <=> p.id
    WHERE p.supplier_name LIKE '%Gmb%'
)
SELECT
    s.id,
    s.price,
    s.cost,
    s.supplier_id,
    s.inventory_quantity,
    s.city,
    CASE WHEN city RLIKE '^(Singapore|Berlin)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS city_zone,
    (SELECT MIN(t2.price) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS min_product_price
FROM joined s
ORDER BY supplier_id DESC
