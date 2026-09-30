WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        b.rating,
        b.supplier_name,
        b.country
    FROM products src
    INNER JOIN suppliers b
        ON src.supplier_id <=> b.id
    WHERE b.city <> 'Singapore'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.price,
        a.stock_qty,
        a.supplier_id,
        a.supplier_name,
        a.country,
        d.city,
        d.rating AS supplier_rating
    FROM with_dims a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_revenue
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(price, 2) OVER (PARTITION BY category ORDER BY supplier_rating, id) next_price,
        AVG(price) OVER (PARTITION BY category) category_avg_price,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY city, category ORDER BY supplier_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
        MAX(price) OVER (PARTITION BY country) country_max_price
    FROM with_latest
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.country AS supplier_country,
        s.supplier_name AS supplier_supplier_name
    FROM enriched src
    INNER JOIN suppliers s
        ON src.supplier_id <=> s.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(category)) category_title,
        CONCAT_WS('-', category, city) category_city_key
    FROM with_dims_products
    WHERE supplier_country IS DISTINCT FROM 'UK'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        stock_qty,
        supplier_name,
        country,
        next_price,
        category_avg_price,
        country_max_price,
        supplier_country,
        category_title,
        category_city_key,
        LAG(stock_qty, 2) OVER w_running prev_stock_qty,
        SUM(next_price) OVER (PARTITION BY supplier_country) supplier_country_sum_next_price,
        LAG(country_max_price, 1) OVER (PARTITION BY category, city ORDER BY price, id) prev_country_max_price,
        ROW_NUMBER() OVER w_hist event_seq,
        AVG(stock_qty) OVER w_ordered stock_qty_avg_5_prior
    FROM cleaned
    WINDOW w_running AS (ORDER BY price, id), w_hist AS (PARTITION BY supplier_id, city ORDER BY store_sale_revenue, id), w_ordered AS (PARTITION BY country ORDER BY country_max_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
-- Grouped metrics
SELECT
    CASE WHEN stock_qty >= 376 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
    CASE WHEN category_avg_price >= 1060 THEN 'PREMIUM' ELSE 'STANDARD' END AS category_avg_price_tier,
    CASE WHEN prev_country_max_price >= 1274 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_country_max_price_tier,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    COUNT(*) AS row_count,
    AVG(stock_qty) AS avg_stock_qty,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(supplier_country)) AS num_supplier_countries
FROM scored
GROUP BY stock_qty_tier, category_avg_price_tier, prev_country_max_price_tier
ORDER BY stock_qty_tier DESC NULLS FIRST
