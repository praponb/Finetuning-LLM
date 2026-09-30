WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT s.*, d.city
    FROM products s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost, 1) OVER w_hist AS next_cost,
        CORR(cost, stock_qty) OVER w_part AS corr_cost_stock_qty
    FROM with_dims
    WINDOW w_hist AS (ORDER BY cost, id), w_part AS (PARTITION BY brand)
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.units,
        x.store_sale_revenue
    FROM enriched s
    JOIN LATERAL (
            SELECT c.units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT category, supplier_id
    FROM latest_match
    EXCEPT ALL
    SELECT category, supplier_id
    FROM products
    WHERE SIZE(tags) >= 2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN matched_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
)
-- Grouped metrics
SELECT
    city,
    CASE WHEN store_sale_revenue >= 3300 THEN 'HIGH' ELSE 'LOW' END AS store_sale_revenue_tier,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN brand = 'Acme' THEN next_cost ELSE 0 END) AS acme_next_cost,
    STDDEV_SAMP(price) AS price_stddev
FROM reconciled
GROUP BY city, CASE WHEN store_sale_revenue >= 3300 THEN 'HIGH' ELSE 'LOW' END
ORDER BY london_count;
