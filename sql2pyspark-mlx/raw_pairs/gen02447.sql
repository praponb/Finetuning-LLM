WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label
    FROM store_sales
    WHERE sale_date > DATE '2023-05-31'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(units, 2, 0) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_units,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY units DESC) AS units_cume_dist,
        NTILE(4) OVER (PARTITION BY YEAR(sale_date), store_id ORDER BY units DESC, id) AS units_bucket,
        CORR(revenue, units) OVER (PARTITION BY product_id) AS corr_revenue_units
    FROM valid
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.revenue,
        e.prev_units,
        e.units_cume_dist,
        e.corr_revenue_units,
        s.store_name,
        s.opened_date
    FROM with_windows e
    LEFT JOIN stores s
        ON e.store_id = s.id
    WHERE s.region NOT IN ('South', 'East', 'West')
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.city,
        p.store_name AS store_store_name,
        p.sqft
    FROM enriched e
    INNER JOIN stores p
        ON e.store_id = p.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.prev_units,
        src.store_name,
        src.opened_date,
        src.city,
        src.sqft,
        p.opened_date AS store_opened_date
    FROM enriched_store_sales src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
enriched_store_sales_2 AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.prev_units,
        e.opened_date,
        e.city,
        p.launch_date,
        p.cost,
        p.stock_qty
    FROM joined e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.opened_date,
    s.city,
    s.launch_date,
    s.cost,
    CASE WHEN city RLIKE '^(London|Tokyo)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS city_zone,
    (SELECT SUM(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS sum_store_sale_units
FROM enriched_store_sales_2 s;
