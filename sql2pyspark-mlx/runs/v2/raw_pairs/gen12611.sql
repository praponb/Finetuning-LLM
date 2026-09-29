SELECT *, CAST(sqft AS STRING) AS sqft_num, UPPER(city) AS city_upper
FROM stores
ORDER BY opened_date
