WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(launch_date, 44) AS launch_date_minus_44d,
        LENGTH(product_name) AS product_name_length
    FROM products
    WHERE launch_date > TO_DATE('2023-04-15')
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.brand,
        a.price,
        a.stock_qty,
        a.launch_date,
        a.supplier_id,
        p.city,
        p.supplier_name,
        p.rating
    FROM filtered a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(stock_qty) OVER (PARTITION BY brand) AS brand_avg_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY MONTH(launch_date), supplier_id ORDER BY launch_date DESC, id) AS event_seq
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        price,
        stock_qty,
        supplier_id,
        rating,
        brand_avg_stock_qty,
        MAX(brand_avg_stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_brand_avg_stock_qty,
        MIN(brand_avg_stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_brand_avg_stock_qty,
        AVG(brand_avg_stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_brand_avg_stock_qty
    FROM scored
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.sale_date,
        x.revenue
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN stock_qty >= 331 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
        supplier_id,
        CORR(price, stock_qty) AS corr_price_stock_qty,
        COUNT(DISTINCT category) AS distinct_categories,
        ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Home' THEN 1 END), 0), 2) AS stock_qty_per_home,
        SUM(CASE WHEN category = 'Fashion' THEN rating ELSE 0 END) AS fashion_rating,
        GROUPING_ID() AS gid
    FROM latest_match
    GROUP BY GROUPING SETS ((CASE WHEN stock_qty >= 331 THEN 'PREMIUM' ELSE 'STANDARD' END, supplier_id), (CASE WHEN stock_qty >= 331 THEN 'PREMIUM' ELSE 'STANDARD' END))
)
SELECT
    s.stock_qty_tier,
    s.supplier_id,
    s.corr_price_stock_qty,
    s.distinct_categories,
    s.stock_qty_per_home,
    s.fashion_rating,
    s.gid,
    (SELECT MAX(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_cost
FROM stats s
ORDER BY supplier_id DESC NULLS FIRST
