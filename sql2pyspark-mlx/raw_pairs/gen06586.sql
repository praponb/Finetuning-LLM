SELECT
    id,
    store_name,
    opened_date,
    ROUND(sqft * 1.07, 2) AS adjusted_sqft
FROM stores
WHERE LENGTH(store_name) < 12
