WITH recent_stores AS (
    SELECT *, INITCAP(TRIM(region)) AS region_title
    FROM stores
    WHERE city <> 'New York' AND city <> 'New York'
),
stores_stats AS (
    SELECT
        QUARTER(opened_date) AS opened_quarter,
        city,
        MIN(opened_date) AS first_opened_date,
        MAX(opened_date) AS last_opened_date
    FROM recent_stores
    GROUP BY QUARTER(opened_date), city
)
SELECT
    opened_quarter,
    city,
    first_opened_date,
    last_opened_date
FROM stores_stats;
