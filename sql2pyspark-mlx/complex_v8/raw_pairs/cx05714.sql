WITH recent_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        QUARTER(opened_on) AS opened_on_quarter,
        DATEDIFF(DATE '2026-01-01', opened_on) AS days_since_opened_on,
        CASE WHEN city IN ('Kuala Lumpur', 'Bangkok') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter_2
    FROM recent_warehouses
    WHERE capacity >= 2614
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT city_group, city, opened_on_quarter
    FROM enriched
) src
PIVOT (
    MIN(opened_on_quarter)
    FOR city IN ('Bangkok', 'Singapore')
)
