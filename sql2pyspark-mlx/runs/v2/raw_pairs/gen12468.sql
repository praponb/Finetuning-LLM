SELECT id, region, CAST(sqft AS DOUBLE) AS sqft_num
FROM stores
WHERE region = 'North' OR sqft >= 4535;
