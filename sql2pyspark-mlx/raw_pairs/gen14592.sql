WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        units,
        DATE_TRUNC('year', sale_date) AS sale_date_period
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-03-28')
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.units,
        s.sale_date_period,
        p2.opened_date
    FROM filtered s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
    WHERE YEAR(p2.opened_date) = 2023
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(units) AS units_whole,
        DATE_FORMAT(sale_date_period, 'yyyy-MM-dd') AS sale_date_period_label
    FROM enriched
    WHERE YEAR(opened_date) >= 2023
),
with_dims AS (
    -- Attach stores attributes
    SELECT src.*, s.opened_date AS store_opened_date
    FROM cleaned src
    JOIN stores s
        ON src.store_id = s.id
)
-- Ranking and gap features
SELECT
    *,
    SUM(units) OVER w_hist AS last4_sum_units,
    MIN(units) OVER w_hist AS last4_min_units,
    MAX(units) OVER w_hist AS last4_max_units
FROM with_dims
WINDOW w_hist AS (PARTITION BY store_id ORDER BY units_whole, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
