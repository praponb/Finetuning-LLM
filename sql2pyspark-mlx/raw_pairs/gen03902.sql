WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CAST(stock_qty AS STRING) AS stock_qty_num
    FROM products
    WHERE launch_date >= DATE '2023-04-04'
      AND brand IS DISTINCT FROM 'Initech'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock <= s.launch_date
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        brand,
        cost,
        launch_date,
        supplier_id,
        stock_qty_num,
        CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END AS is_grocery
    FROM not_flagged
    WHERE supplier_id IN (8, 31, 33)
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.store_sale_units
    FROM base s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost ASC) AS cost_dense_rank,
        AVG(is_grocery) OVER (PARTITION BY category, brand ORDER BY launch_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_grocery_avg_6_prior,
        AVG(store_sale_revenue) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS store_sale_revenue_avg_2_prior,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date) ORDER BY cost DESC NULLS LAST) AS cost_rank,
        COUNT(cost) OVER (PARTITION BY MONTH(launch_date), brand) AS launch_date_month_count_cost
    FROM with_last
    WHERE store_sale_revenue < 1800
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(store_sale_units) OVER (PARTITION BY brand ORDER BY is_grocery_avg_6_prior ASC, id ASC) AS prev_store_sale_units,
        LEAD(store_sale_units) OVER (PARTITION BY brand ORDER BY is_grocery_avg_6_prior ASC, id ASC) AS next_store_sale_units,
        LEAD(is_grocery_avg_6_prior) OVER (PARTITION BY brand ORDER BY is_grocery_avg_6_prior ASC, id ASC) AS next_is_grocery_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY is_grocery_avg_6_prior ASC, id ASC) AS row_seq,
        NTILE(3) OVER (PARTITION BY brand ORDER BY cost_rank ASC, id ASC) AS cost_rank_bucket
    FROM with_windows
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY store_sale_revenue_avg_2_prior DESC, id ASC) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
)
-- Join a dimension and aggregate
SELECT
    p.city,
    p.country,
    e.brand,
    COUNT(*) AS num_rows,
    SUM(e.prev_store_sale_units) AS total_prev_store_sale_units,
    ROUND(SUM(e.cost_rank), 2) AS total_cost_rank,
    GROUPING_ID() AS grouping_level
FROM latest e
LEFT JOIN suppliers p ON e.supplier_id = p.id
GROUP BY GROUPING SETS ((p.city, p.country, e.brand), (p.city), ());
