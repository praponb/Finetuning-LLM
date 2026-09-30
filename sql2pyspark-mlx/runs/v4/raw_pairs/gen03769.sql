WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        CASE WHEN brand IN ('Umbrella', 'Initech') THEN 'Priority' ELSE 'Other' END AS brand_group,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM products
    WHERE launch_date >= TO_DATE('2023-11-09')
      AND supplier_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        brand_group,
        resolved_brand,
        LAG(stock_qty, 1) OVER (PARTITION BY brand, category ORDER BY stock_qty, id) AS prev_stock_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share
    FROM cleaned
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.units BETWEEN 15 AND 16
        )
),
scored_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(stock_qty) OVER w_ordered AS stock_qty_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_share DESC, id) AS event_seq
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY supplier_id, category ORDER BY price_share, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.city,
        d.supplier_name
    FROM scored_products e
    JOIN suppliers d
        ON e.supplier_id = d.id
),
joined AS (
    -- Attach suppliers attributes
    SELECT a.*, p.supplier_name AS supplier_supplier_name
    FROM enriched a
    JOIN suppliers p
        ON a.supplier_id = p.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_stock_qty >= 306 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_stock_qty_tier,
        COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
        APPROX_PERCENTILE(prev_stock_qty, 0.9) AS prev_stock_qty_p90
    FROM joined
    GROUP BY CASE WHEN prev_stock_qty >= 306 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT prev_stock_qty_tier, new_york_count
FROM totals
ORDER BY new_york_count DESC NULLS LAST;
