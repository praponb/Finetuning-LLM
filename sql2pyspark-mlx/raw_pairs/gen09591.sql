WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(store_name) clean_store_name,
        INITCAP(TRIM(region)) region_title
    FROM stores
    WHERE opened_date > DATE '2023-03-06'
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.store_sale_revenue
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.opened_date,
        s.sqft,
        s.clean_store_name,
        s.region_title,
        s.store_sale_sale_date,
        s.store_sale_revenue,
        top1.inventory_quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        clean_store_name,
        store_sale_sale_date,
        store_sale_revenue,
        inventory_quantity,
        AVG(sqft) OVER (PARTITION BY city ORDER BY store_sale_revenue, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) sqft_avg_2_prior,
        AVG(store_sale_revenue) OVER (PARTITION BY city ORDER BY inventory_quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) store_sale_revenue_avg_5,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date), city ORDER BY sqft DESC, id) sqft_bucket
    FROM latest_match
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.product_id IN (1, 3, 4, 9)
        )
)
SELECT
    id,
    city,
    store_sale_sale_date,
    store_sale_revenue,
    inventory_quantity,
    sqft_avg_2_prior,
    store_sale_revenue_avg_5,
    CASE WHEN city RLIKE '^(London|Singapore)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS city_zone
FROM not_flagged
