WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date >= s.opened_date
        )
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_units
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units = 12
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        INITCAP(TRIM(store_name)) AS store_name_title,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM kept
    WHERE opened_date > DATE '2023-02-19'
),
kept_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.opened_date_month_end
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        sqft,
        store_name_title,
        MAX(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft,
        MIN(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
    FROM kept_stores
)
-- Grouped metrics
SELECT
    city,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    PERCENTILE_APPROX(running_min_sqft, 0.95) AS running_min_sqft_p95,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS sqft_per_new_york
FROM enriched
GROUP BY 1;
