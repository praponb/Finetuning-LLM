SELECT
    id,
    sqft,
    YEAR(opened_date) AS opened_date_year,
    FLOOR(sqft) AS sqft_whole
FROM stores
WHERE sqft >= 7389
