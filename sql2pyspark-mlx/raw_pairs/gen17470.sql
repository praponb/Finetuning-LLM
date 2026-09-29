WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        units,
        revenue,
        CASE WHEN revenue >= 3100 THEN 'High' WHEN revenue >= 1700 THEN 'Medium' ELSE 'Low' END AS revenue_tier,
        ROUND(revenue / units, 2) AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-05-23')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM store_sales
    WHERE units = 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.store_id = k.store_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share_2
    FROM kept_rows
    WHERE units = 39
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(units_share, corr_revenue_units) AS greatest_units_share_corr_revenue_units
    FROM metrics
    WHERE units_share >= 0.61
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.corr_revenue_units,
        e.units_share,
        e.units_share_2,
        e.greatest_units_share_corr_revenue_units,
        p.opened_date,
        p.store_name
    FROM base e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT src.*, d.sqft
    FROM enriched src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(greatest_units_share_corr_revenue_units) OVER (PARTITION BY YEAR(opened_date), store_id) AS opened_date_year_min_greatest_units_share_corr_revenue_units,
        RANK() OVER (PARTITION BY store_id ORDER BY corr_revenue_units NULLS LAST) AS corr_revenue_units_rank,
        CUME_DIST() OVER (ORDER BY greatest_units_share_corr_revenue_units) AS greatest_units_share_corr_revenue_units_cume_dist
    FROM enriched_store_sales
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN greatest_units_share_corr_revenue_units >= 79 THEN 'LARGE' ELSE 'SMALL' END AS greatest_units_share_corr_revenue_units_tier,
    SUM(units_share_2) AS total_units_share_2,
    COUNT(*) AS num_rows,
    SUM(opened_date_year_min_greatest_units_share_corr_revenue_units) AS total_opened_date_year_min_greatest_units_share_corr_revenue_units,
    ROUND(AVG(units_share), 2) AS avg_units_share,
    MAX(opened_date_year_min_greatest_units_share_corr_revenue_units) AS max_opened_date_year_min_greatest_units_share_corr_revenue_units
FROM with_windows
GROUP BY CASE WHEN greatest_units_share_corr_revenue_units >= 79 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY total_opened_date_year_min_greatest_units_share_corr_revenue_units;
