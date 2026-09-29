WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM store_sales
    WHERE store_id = 5
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        sale_date_period,
        GREATEST(revenue, units) AS greatest_revenue_units
    FROM valid
    WHERE sale_date >= TO_DATE('2023-07-21')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY greatest_revenue_units, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.units,
        e.revenue,
        e.sale_date_period,
        e.greatest_revenue_units,
        b.sqft,
        b.store_name,
        b.opened_date
    FROM ranked e
    INNER JOIN stores b
        ON e.store_id = b.id
    WHERE b.sqft = 12995
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.units,
        a.revenue,
        a.sale_date_period,
        a.store_name,
        b.sqft AS store_sqft
    FROM with_dims a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date_period, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        units,
        revenue,
        sale_date_period,
        store_name,
        row_num,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY units DESC, id) AS units_bucket,
        LAST_VALUE(store_sqft) IGNORE NULLS OVER (PARTITION BY MONTH(sale_date_period), store_id ORDER BY sale_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_store_sqft
    FROM ranked_store_sales
)
SELECT
    id,
    store_id,
    units,
    revenue,
    sale_date_period,
    store_name,
    row_num,
    last_known_store_sqft,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Old Town)$' THEN 'EXTENDED' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS store_name_zone
FROM metrics
ORDER BY store_id DESC NULLS FIRST;
