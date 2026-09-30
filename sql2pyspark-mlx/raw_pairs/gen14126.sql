WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(price) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
        NTILE(5) OVER (PARTITION BY category ORDER BY stock_qty, id) AS stock_qty_bucket,
        AVG(stock_qty) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_5_prior,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), category ORDER BY cost NULLS FIRST) AS cost_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY YEAR(launch_date), category ORDER BY price, id) AS seq_no
    FROM products
    WHERE price >= 320
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        d.supplier_name,
        d.rating,
        d.country
    FROM with_windows s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost_cume_dist NULLS FIRST, id) = 1
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        b.supplier_name AS supplier_supplier_name,
        b.country AS supplier_country,
        b.city
    FROM latest s
    LEFT JOIN suppliers b
        ON s.supplier_id = b.id
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.stock_qty_bucket,
        s.stock_qty_avg_5_prior,
        s.cost_cume_dist,
        s.rating,
        s.supplier_supplier_name,
        s.supplier_country,
        x.store_sale_sale_date
    FROM enriched_products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(stock_qty) OVER w_ordered AS next_stock_qty,
        LAG(stock_qty) OVER w_ordered AS prev_stock_qty,
        LAG(stock_qty_avg_5_prior) OVER w_ordered AS prev_stock_qty_avg_5_prior,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        LEAD(stock_qty_avg_5_prior, 1, 0) OVER (PARTITION BY supplier_country ORDER BY stock_qty_avg_5_prior, id) AS next_stock_qty_avg_5_prior
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY category ORDER BY stock_qty_bucket, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY stock_qty_avg_5_prior DESC NULLS FIRST) AS stock_qty_avg_5_prior_cume_dist,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY next_stock_qty DESC NULLS LAST) AS next_stock_qty_dense_rank,
        LAST_VALUE(stock_qty_bucket, TRUE) OVER (PARTITION BY category, supplier_country ORDER BY cost_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty_bucket
    FROM scored
),
summary AS (
    -- Grouped metrics
    SELECT
        category,
        CASE WHEN price >= 1140 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
        ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Books' THEN 1 END), 0), 2) AS stock_qty_per_books,
        AVG(stock_qty_avg_5_prior) FILTER (WHERE category = 'Electronics') AS electronics_avg_stock_qty_avg_5_prior,
        ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN supplier_country = 'TH' THEN 1 END), 0), 2) AS stock_qty_per_th,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN supplier_country = 'TH' THEN 1 END), 0), 2) DESC) AS stock_qty_per_th_percent_rank
    FROM metrics
    GROUP BY 1, 2
)
SELECT
    category,
    price_tier,
    stock_qty_per_books,
    electronics_avg_stock_qty_avg_5_prior,
    stock_qty_per_th
FROM summary;
