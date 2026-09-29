SELECT
    id,
    store_name,
    sqft,
    opened_date,
    CAST(sqft AS STRING) AS sqft_str
FROM stores
WHERE region NOT IN ('North', 'East')
