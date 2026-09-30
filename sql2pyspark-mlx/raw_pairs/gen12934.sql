WITH joined AS (
    SELECT
        src.*,
        d.stock_qty,
        d.category,
        d.price
    FROM store_sales src
    LEFT JOIN products d
        ON src.product_id = d.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY price DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
),
enriched AS (
    SELECT
        a.id,
        a.store_id,
        a.sale_date,
        a.revenue,
        a.stock_qty,
        a.category,
        b.store_name
    FROM latest a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
base AS (
    SELECT
        id,
        store_id,
        revenue,
        stock_qty,
        category,
        stock_qty + revenue AS stock_qty_plus_revenue,
        COALESCE(category, 'UNKNOWN') AS resolved_category
    FROM enriched
    WHERE category IS NOT NULL
)
SELECT
    id,
    store_id,
    metric_name,
    val
FROM (
    SELECT
        id,
        store_id,
        stock_qty_plus_revenue,
        revenue
    FROM base
) src
UNPIVOT (
    val FOR metric_name IN (stock_qty_plus_revenue, revenue)
) unp;
