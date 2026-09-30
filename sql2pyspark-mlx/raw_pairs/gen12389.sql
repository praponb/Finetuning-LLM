WITH filtered AS (
    SELECT
        id,
        city,
        region,
        sqft,
        CASE WHEN city IN ('Bangkok', 'Chiang Mai') THEN 'Group A' ELSE 'Other' END AS city_group,
        CONCAT_WS('-', region, store_name) AS region_store_name_key,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND LENGTH(store_name) < 6
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.units BETWEEN 30 AND 36
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.product_id IN (1, 2, 9, 10)
        )
),
latest_match AS (
    SELECT
        s.*,
        x.store_sale_sale_date,
        x.units
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY store_sale_sale_date) AS row_num
    FROM latest_match
    QUALIFY row_num = 1
),
eligible_stores AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.store_sale_sale_date
        )
)
SELECT
    id,
    city,
    region,
    sqft,
    city_group,
    region_store_name_key,
    store_sale_sale_date,
    row_num
FROM eligible_stores
ORDER BY city
