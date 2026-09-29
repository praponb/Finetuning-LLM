WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(revenue, units) AS greatest_revenue_units
    FROM store_sales
    WHERE sale_date > DATE '2023-03-02'
      AND units <= 23
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC NULLS FIRST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY units DESC, id) AS units_bucket,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_avg_3
    FROM deduped
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        sale_date,
        units,
        rank_in_group,
        units_bucket,
        revenue_avg_3
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_avg_3 DESC, id) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.units_bucket,
        d.product_name,
        d.stock_qty
    FROM latest src
    LEFT JOIN products d
        ON src.product_id = d.id
    WHERE d.stock_qty < 150
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        units_bucket,
        stock_qty,
        LAG(units_bucket) OVER (PARTITION BY product_id ORDER BY units_bucket, id) AS prev_units_bucket,
        LEAD(units_bucket) OVER (PARTITION BY product_id ORDER BY units_bucket, id) AS next_units_bucket,
        LEAD(stock_qty) OVER (PARTITION BY product_id ORDER BY units_bucket, id) AS next_stock_qty,
        LAG(stock_qty) OVER (PARTITION BY product_id ORDER BY units_bucket, id) AS prev_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_bucket, id) AS row_seq,
        LAG(units_bucket, 1) OVER (ORDER BY units_bucket, id) AS prev_units_bucket_2
    FROM with_dims
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        units_bucket,
        next_units_bucket,
        next_stock_qty,
        prev_stock_qty,
        row_seq,
        LAG(prev_units_bucket_2, 1) OVER (PARTITION BY product_id ORDER BY row_seq, id) AS prev_prev_units_bucket_2,
        SUM(prev_stock_qty) OVER (PARTITION BY product_id ORDER BY units_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_stock_qty
    FROM scored
)
SELECT
    s.id,
    s.product_id,
    s.units_bucket,
    s.next_stock_qty,
    s.row_seq,
    s.cumulative_prev_stock_qty,
    (SELECT AVG(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS avg_inventory_quantity
FROM with_windows s
ORDER BY units_bucket DESC, id
LIMIT 10;
