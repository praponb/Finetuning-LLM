SELECT
    id,
    city,
    region,
    store_name,
    DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
FROM stores
WHERE opened_date BETWEEN '2024-07-01' AND '2025-03-01' AND store_name IS NOT NULL AND sqft = 12716;
