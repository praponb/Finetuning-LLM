SELECT
    id,
    store_name,
    opened_date,
    sqft,
    LOWER(store_name) store_name_lower,
    ROUND(sqft, 1) sqft_rounded
FROM stores
WHERE store_name IS NOT NULL OR opened_date >= '2025-06-01'
