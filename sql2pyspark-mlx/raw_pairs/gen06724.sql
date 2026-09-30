WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        DATE_SUB(launch_date, 22) AS launch_date_minus_22d
    FROM products
    WHERE launch_date > TO_DATE('2023-03-05')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        launch_date_minus_22d,
        LAG(stock_qty, 1, 0) OVER w_hist AS prev_stock_qty,
        AVG(cost) OVER (PARTITION BY category, brand) AS category_avg_cost,
        LAG(price, 2) OVER (ORDER BY launch_date ASC, id ASC) AS prev_price,
        NTILE(3) OVER w_ordered AS cost_bucket,
        SUM(price) OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price
    FROM cleaned
    WHERE brand IS NOT NULL
    WINDOW w_hist AS (PARTITION BY brand, category ORDER BY cost ASC, id ASC), w_ordered AS (PARTITION BY category, supplier_id ORDER BY cost ASC, id ASC)
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.cost,
        s.launch_date,
        top1.inventory_last_restock
    FROM scored s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        COUNT(cost) OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        MIN(cost) OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('week', inventory_last_restock), category ORDER BY cost ASC, id ASC) AS cost_bucket
    FROM latest_match
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY inventory_last_restock DESC, id ASC) AS dedup_rank
    FROM scored_products
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    category,
    cost,
    launch_date,
    inventory_last_restock,
    running_avg_cost,
    running_count_cost,
    running_min_cost,
    cost_bucket,
    CASE WHEN category RLIKE '^(Home|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS category_zone
FROM latest
ORDER BY inventory_last_restock DESC NULLS FIRST
