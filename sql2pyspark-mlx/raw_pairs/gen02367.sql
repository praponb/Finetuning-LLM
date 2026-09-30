WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock,
        DATE_SUB(last_restock, 27) AS last_restock_minus_27d
    FROM inventory
    WHERE last_restock BETWEEN '2023-04-01' AND '2023-09-15'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        days_since_last_restock,
        SUM(days_since_last_restock) OVER (PARTITION BY product_id ORDER BY days_since_last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id ASC) AS seq_no,
        NTILE(10) OVER (PARTITION BY product_id, store_id ORDER BY quantity ASC, id ASC) AS quantity_bucket,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id ORDER BY last_restock_minus_27d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM cleaned
    WHERE quantity IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM inventory
    WHERE product_id = 8
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.stock_qty,
        b.launch_date,
        b.brand
    FROM verified e
    LEFT JOIN products b
        ON e.product_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(stock_qty) OVER (PARTITION BY brand) AS brand_min_stock_qty,
        RANK() OVER (PARTITION BY MONTH(launch_date), brand ORDER BY cumulative_days_since_last_restock ASC NULLS LAST) AS cumulative_days_since_last_restock_rank,
        PERCENT_RANK() OVER w_recent AS stock_qty_percent_rank
    FROM joined
    WINDOW w_recent AS (ORDER BY stock_qty ASC NULLS FIRST)
)
-- Join a dimension and aggregate
SELECT
    d.brand AS product_brand,
    a.brand AS inventory_brand,
    COUNT(*) AS num_rows,
    MAX(a.quantity_bucket) AS max_quantity_bucket,
    GROUPING_ID() AS grouping_level
FROM with_windows a
LEFT JOIN products d ON a.product_id = d.id
GROUP BY ROLLUP (d.brand, a.brand)
ORDER BY product_brand ASC NULLS LAST
