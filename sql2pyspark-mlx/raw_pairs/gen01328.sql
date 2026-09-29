WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        TRIM(city) clean_city,
        COALESCE(region, 'NONE') resolved_region
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'Central'
      AND opened_date >= TO_DATE('2023-06-18')
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) AS rn
FROM cleaned
QUALIFY rn = 1
