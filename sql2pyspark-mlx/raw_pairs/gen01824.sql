SELECT ROUND(STDDEV(sqft), 2) stddev_sqft, COUNT(sqft) sqft_count
FROM stores
WHERE (store_name LIKE '%Tow%' AND opened_date >= '2023-06-01') OR store_name LIKE '%Flagshi%';
