WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost / price, 2) AS margin,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE brand IS NOT DISTINCT FROM 'Stark'
      AND ARRAY_CONTAINS(tags, 'sale')
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.revenue
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.cost,
        src.stock_qty,
        src.launch_date,
        src.supplier_id,
        src.margin,
        src.resolved_brand,
        src.revenue,
        b.rating,
        b.city
    FROM latest_match src
    INNER JOIN suppliers b
        ON src.supplier_id = b.id
    WHERE b.city NOT IN ('London', 'Chiang Mai')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id = 26
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        LEAD(margin) OVER (PARTITION BY city ORDER BY rating, id) AS next_margin,
        LAG(margin) OVER (PARTITION BY city ORDER BY rating, id) AS prev_margin,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no
    FROM eligible
),
enriched_products AS (
    -- Running and moving aggregates
    SELECT
        id,
        stock_qty,
        launch_date,
        supplier_id,
        revenue,
        next_margin,
        prev_margin,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY margin, id) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY supplier_id, city ORDER BY prev_rating NULLS FIRST) AS prev_rating_percent_rank,
        LAST_VALUE(seq_no, TRUE) OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no
    FROM enriched
)
-- Window metrics over several different partitions
SELECT
    id,
    stock_qty,
    launch_date,
    supplier_id,
    next_margin,
    prev_margin,
    prev_rating_percent_rank,
    last_known_seq_no,
    SUM(last_known_seq_no) OVER (PARTITION BY supplier_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_seq_no,
    NTILE(10) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS stock_qty_bucket
FROM enriched_products
