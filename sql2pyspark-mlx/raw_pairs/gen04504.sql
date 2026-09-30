WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_stock_qty,
        top1.cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM with_latest
    INTERSECT
    SELECT id, city
    FROM suppliers
    WHERE rating >= 2.2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        supplier_name_length,
        product_stock_qty,
        CUME_DIST() OVER w_part AS cost_cume_dist,
        MAX(product_stock_qty) OVER w_hist AS country_max_product_stock_qty
    FROM reconciled
    WHERE country IN ('DE', 'US')
    WINDOW w_part AS (PARTITION BY city, country ORDER BY cost DESC), w_hist AS (PARTITION BY country)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(country_max_product_stock_qty) OVER w_recent AS city_avg_country_max_product_stock_qty,
        RANK() OVER (ORDER BY cost_cume_dist DESC NULLS FIRST) AS cost_cume_dist_rank,
        AVG(product_stock_qty) OVER w_recent AS city_avg_product_stock_qty
    FROM enriched
    WINDOW w_recent AS (PARTITION BY city)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM scored
    INTERSECT ALL
    SELECT city, id
    FROM suppliers
    WHERE supplier_name LIKE 'Si%'
),
reconciled_suppliers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN city_avg_product_stock_qty >= 312 THEN 'LARGE' ELSE 'SMALL' END AS city_avg_product_stock_qty_tier,
    PERCENTILE_APPROX(supplier_name_length, 0.95) AS supplier_name_length_p95,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows
FROM reconciled_suppliers
GROUP BY CASE WHEN city_avg_product_stock_qty >= 312 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY berlin_rows DESC NULLS LAST;
