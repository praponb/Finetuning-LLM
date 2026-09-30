WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_revenue
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND MONTH(ch.last_restock) IN (7, 8, 11)
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, id
    FROM not_flagged
    INTERSECT
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    supplier_id,
    PERCENTILE_APPROX(price, 0.9) AS price_p90,
    ROUND(AVG(store_sale_revenue), 2) AS avg_store_sale_revenue,
    ROUND(SUM(store_sale_revenue) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) AS store_sale_revenue_per_sports
FROM verified
GROUP BY CUBE (DATE_TRUNC('QUARTER', launch_date), supplier_id)
HAVING avg_store_sale_revenue > 0
