SELECT MAX(sqft) - MIN(sqft) AS sqft_range, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE sqft != 10132 OR sqft < 12091
