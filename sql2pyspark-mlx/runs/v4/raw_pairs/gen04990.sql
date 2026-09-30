WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE opened_date >= DATE '2023-05-24'
      AND opened_date >= '2023-07-01'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM base
) src
PIVOT (
    AVG(sqft)
    FOR city IN ('New York' AS new_york, 'Chiang Mai' AS chiang_mai, 'London' AS london)
);
