WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        DATE_TRUNC('year', launch_date) AS launch_date_period,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE stock_qty = 398
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.unit_price,
        x.quantity
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        launch_date,
        resolved_brand,
        unit_price,
        LEAD(price, 1) OVER w_recent AS next_price,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY stock_qty NULLS FIRST) AS stock_qty_percent_rank
    FROM latest_match
    WINDOW w_recent AS (PARTITION BY brand ORDER BY price, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        price,
        cost,
        launch_date,
        resolved_brand,
        next_price,
        stock_qty_percent_rank,
        SUM(stock_qty_percent_rank) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty_percent_rank,
        AVG(next_price) OVER (PARTITION BY category ORDER BY next_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_price_avg_6_prior
    FROM ranked
),
matched_keys AS (
    -- Set difference on keys
    SELECT category, id
    FROM metrics
    EXCEPT ALL
    SELECT category, id
    FROM products
    WHERE category = 'Books'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.category = k.category AND s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(launch_date, 72) AS launch_date_minus_72d
    FROM reconciled
    WHERE cost BETWEEN 420 AND 650
),
summary AS (
    -- Multi-level aggregation
    SELECT
        category,
        SIZE(COLLECT_SET(category)) AS num_categories,
        ROUND(AVG(next_price_avg_6_prior), 2) AS avg_next_price_avg_6_prior,
        SUM(price) AS total_price,
        COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(stock_qty_percent_rank) AS max_stock_qty_percent_rank
    FROM base
    GROUP BY category
)
SELECT
    category,
    num_categories,
    total_price,
    sports_rows,
    max_stock_qty_percent_rank
FROM summary
