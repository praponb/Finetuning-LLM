WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        price,
        stock_qty,
        launch_date,
        CONCAT_WS(',', product_name, category) AS product_name_category_key,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2023-06-02'
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name_category_key,
        s.resolved_brand,
        latest.store_sale_units,
        latest.store_sale_sale_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.last_restock
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(store_sale_units, 1, 0) OVER (ORDER BY last_restock ASC, id ASC) AS next_store_sale_units,
        ROW_NUMBER() OVER (ORDER BY store_sale_sale_date DESC, id ASC) AS event_seq
    FROM latest_match
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    DATE_TRUNC('WEEK', store_sale_sale_date) AS week_start,
    DATE_TRUNC('YEAR', store_sale_sale_date) AS year_start,
    APPROX_PERCENTILE(event_seq, 0.5) AS event_seq_p50,
    COUNT(*) AS cnt,
    ROUND(AVG(next_store_sale_units), 2) AS avg_next_store_sale_units,
    GROUPING(CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', store_sale_sale_date), DATE_TRUNC('YEAR', store_sale_sale_date)), (CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', store_sale_sale_date)));
