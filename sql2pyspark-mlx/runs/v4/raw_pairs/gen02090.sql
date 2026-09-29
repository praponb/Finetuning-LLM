WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        FLOOR(revenue) AS revenue_whole,
        GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE sale_date >= DATE '2024-04-30'
),
with_dims AS (
    SELECT a.*, p.region
    FROM valid a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
joined AS (
    SELECT
        a.*,
        p.launch_date,
        p.cost,
        p.brand
    FROM with_dims a
    INNER JOIN products p
        ON a.product_id = p.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY launch_date, id) AS latest_rank
        FROM joined
    ) x
    WHERE latest_rank = 1
),
valid_store_sales AS (
    SELECT
        *,
        units + greatest_units_revenue AS units_plus_greatest_units_revenue,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM deduped
    WHERE region IS NOT NULL
),
joined_store_sales AS (
    SELECT src.*, d.sqft
    FROM valid_store_sales src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
summary AS (
    SELECT
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        store_id,
        brand,
        COUNT_IF(region = 'South') AS south_events,
        SUM(CASE WHEN region = 'South' THEN units ELSE 0 END) AS south_units,
        SUM(CASE WHEN region = 'East' THEN greatest_units_revenue ELSE 0 END) AS east_greatest_units_revenue,
        ROUND(SUM(greatest_units_revenue) / NULLIF(COUNT(CASE WHEN brand = 'Initech' THEN 1 END), 0), 2) AS greatest_units_revenue_per_initech,
        COUNT(*) AS cnt,
        MAX(greatest_units_revenue) AS max_greatest_units_revenue,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', sale_date) ORDER BY COUNT_IF(region = 'South') DESC) AS south_events_percent_rank
    FROM joined_store_sales
    GROUP BY DATE_TRUNC('QUARTER', sale_date), store_id, brand
)
SELECT
    s.quarter_start,
    s.store_id,
    s.brand,
    s.south_events,
    s.south_units,
    s.east_greatest_units_revenue,
    s.greatest_units_revenue_per_initech,
    s.cnt,
    s.max_greatest_units_revenue,
    CASE WHEN brand RLIKE '^(Umbrella|Globex)$' THEN 'GROUP_B' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'CORE' ELSE 'EXTENDED' END AS brand_zone,
    (SELECT MAX(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS max_store_sale_units
FROM summary s;
