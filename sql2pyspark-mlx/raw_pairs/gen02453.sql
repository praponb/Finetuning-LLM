WITH filtered AS (
    SELECT
        *,
        DATE_FORMAT(launch_date, 'MMM yyyy') launch_date_label,
        COALESCE(brand, 'OTHER') brand_filled
    FROM products
    WHERE launch_date > TO_DATE('2023-09-08')
      AND brand IS NOT DISTINCT FROM 'Umbrella'
),
with_latest AS (
    SELECT
        s.id,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.supplier_id,
        latest.store_sale_sale_date,
        latest.units
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (1, 7, 8, 9)
        )
),
matched_keys AS (
    SELECT id, supplier_id
    FROM eligible
    INTERSECT ALL
    SELECT id, supplier_id
    FROM products
    WHERE brand IN ('Initech', 'Stark')
),
verified AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
with_dims AS (
    SELECT src.*, p.city
    FROM verified src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
),
grouped AS (
    SELECT
        city,
        COUNT(DISTINCT brand) AS distinct_brands,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
        SIZE(COLLECT_SET(brand)) AS num_brands
    FROM with_dims
    GROUP BY city
)
SELECT
    city,
    distinct_brands,
    london_count
FROM grouped;
