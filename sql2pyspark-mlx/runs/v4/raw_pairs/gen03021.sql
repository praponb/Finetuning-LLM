WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(TRIM(product_name)) AS product_name_normalized
    FROM products
    WHERE supplier_id IS NOT NULL
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.store_sale_sale_date,
        x.revenue
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match_products AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.sale_date,
        latest.units
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.sale_date, c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY revenue, id) AS row_seq,
        FIRST_VALUE(units, TRUE) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units,
        SUM(price) OVER (PARTITION BY category, brand) AS category_sum_price,
        AVG(units) OVER (PARTITION BY supplier_id, brand) AS supplier_id_avg_units
    FROM latest_match_products
)
-- Running and moving aggregates
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    product_name_normalized,
    store_sale_sale_date,
    revenue,
    sale_date,
    units,
    row_seq,
    first_known_units,
    category_sum_price,
    supplier_id_avg_units,
    LAG(stock_qty, 1) OVER (PARTITION BY category ORDER BY row_seq, id) AS prev_stock_qty,
    NTILE(3) OVER (PARTITION BY brand ORDER BY supplier_id_avg_units, id) AS supplier_id_avg_units_bucket,
    SUM(row_seq) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq
FROM scored;
