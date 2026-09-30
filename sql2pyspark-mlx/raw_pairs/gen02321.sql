WITH filtered AS (
    SELECT
        id,
        city,
        region,
        sqft,
        QUARTER(opened_date) AS opened_date_quarter
    FROM stores
    WHERE region IS NOT NULL
      AND store_name IS NULL
)
SELECT id, region
FROM filtered
EXCEPT DISTINCT
SELECT id, region
FROM stores
WHERE region IN ('West', 'Central', 'East')
