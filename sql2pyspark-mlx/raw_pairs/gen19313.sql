SELECT
    id,
    city,
    sqft,
    COALESCE(sqft, 0) AS sqft_filled
FROM stores
WHERE sqft < 5139;
