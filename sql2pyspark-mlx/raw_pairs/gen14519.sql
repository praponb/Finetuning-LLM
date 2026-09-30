WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 years_since_opened_date,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM stores
    WHERE region IS DISTINCT FROM 'South'
      AND opened_date > DATE '2023-06-01'
)
SELECT id, region
FROM filtered
EXCEPT ALL
SELECT id, region
FROM stores
WHERE opened_date <= '2025-08-01'
