WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end,
        revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-05-25'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue ASC NULLS FIRST, id ASC) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY row_num ASC, id ASC) AS prev_revenue_per_unit,
        LEAD(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY row_num ASC, id ASC) AS next_revenue_per_unit,
        LEAD(revenue) OVER (PARTITION BY product_id ORDER BY row_num ASC, id ASC) AS next_revenue,
        LAG(revenue) OVER (PARTITION BY product_id ORDER BY row_num ASC, id ASC) AS prev_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY row_num ASC, id ASC) AS event_seq
    FROM deduped
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(revenue, row_num) AS greatest_revenue_row_num
    FROM enriched
    WHERE sale_date > DATE '2023-09-16'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date,
        p.brand,
        p.stock_qty
    FROM filtered e
    INNER JOIN products p
        ON e.product_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    BIT_OR(units) AS units_bit_or,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM with_dims
GROUP BY product_id
ORDER BY product_id DESC;
