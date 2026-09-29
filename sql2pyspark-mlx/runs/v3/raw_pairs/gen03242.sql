SELECT
    opened_date,
    sqft,
    CAST(sqft AS STRING) AS sqft_num,
    ROUND(sqft * 1.07, 2) AS sqft_scaled
FROM stores
WHERE region = 'South' AND city = 'Chiang Mai'
ORDER BY sqft_num DESC, sqft_scaled DESC;
