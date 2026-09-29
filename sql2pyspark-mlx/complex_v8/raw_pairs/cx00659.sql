WITH filtered_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        DATE_TRUNC('WEEK', opened_on) AS opened_on_week,
        DATEDIFF(DATE '2026-01-01', opened_on) AS days_since_opened_on,
        SUBSTRING(TRIM(wh_code), 1, 4) AS wh_code_prefix
    FROM warehouses
    WHERE city IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        wh_code,
        opened_on,
        days_since_opened_on,
        ADD_MONTHS(opened_on, 1) AS opened_on_plus_1m,
        DATEDIFF(DATE '2025-06-30', opened_on_week) AS days_since_opened_on_week,
        UPPER(TRIM(wh_code)) AS wh_code_clean
    FROM filtered_warehouses
    WHERE city IN ('Singapore', 'Kuala Lumpur')
)
SELECT id, metric_name, value
FROM (
    SELECT id, days_since_opened_on_week, days_since_opened_on
    FROM labeled
) src
UNPIVOT (
    value FOR metric_name IN (days_since_opened_on_week, days_since_opened_on)
) unp
