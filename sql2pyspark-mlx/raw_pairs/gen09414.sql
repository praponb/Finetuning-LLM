WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(price, TRUE) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price,
        CORR(price, cost) OVER (PARTITION BY supplier_id) AS corr_price_cost,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS seq_no,
        LEAD(cost, 1) OVER (PARTITION BY YEAR(launch_date), category ORDER BY price, id) AS next_cost,
        SUM(stock_qty) OVER (PARTITION BY supplier_id, category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
    FROM valid
),
metrics_products AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        brand,
        cost,
        launch_date,
        supplier_id,
        corr_price_cost,
        next_cost,
        running_stock_qty,
        MAX(corr_price_cost) OVER (PARTITION BY category, brand ORDER BY running_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_corr_price_cost,
        AVG(corr_price_cost) OVER (PARTITION BY category, brand ORDER BY running_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_corr_price_cost,
        SUM(corr_price_cost) OVER (PARTITION BY category, brand ORDER BY running_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_corr_price_cost,
        AVG(seq_no) OVER (ORDER BY corr_price_cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_avg_5
    FROM metrics
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.cost,
        s.launch_date,
        s.supplier_id,
        s.running_stock_qty,
        s.running_max_corr_price_cost,
        s.running_avg_corr_price_cost,
        s.seq_no_avg_5,
        latest.revenue,
        latest.units
    FROM metrics_products s
    LEFT JOIN LATERAL (
            SELECT c.revenue, c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY units, id) AS rn
    FROM with_latest
    QUALIFY rn = 1
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.product_name,
        e.cost,
        e.supplier_id,
        e.running_stock_qty,
        e.running_avg_corr_price_cost,
        e.seq_no_avg_5,
        e.units,
        p.rating,
        p.city
    FROM deduped e
    JOIN suppliers p
        ON e.supplier_id <=> p.id
    WHERE p.rating > 3.4
)
SELECT
    s.id,
    s.supplier_id,
    s.running_avg_corr_price_cost,
    s.seq_no_avg_5,
    s.units,
    s.rating,
    s.city,
    CASE WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|Singapore)$' THEN 'EXTENDED' ELSE 'CORE' END AS city_zone,
    (SELECT MIN(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_price
FROM enriched s
ORDER BY min_product_price DESC NULLS FIRST;
