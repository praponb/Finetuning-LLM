SELECT
    store_name,
    city,
    opened_date,
    COALESCE(region, 'Unknown') AS region_filled
FROM stores
WHERE region IN ('Central', 'West', 'North');
