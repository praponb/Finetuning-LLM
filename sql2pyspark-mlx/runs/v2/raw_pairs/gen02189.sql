SELECT id, city, region, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 3) AS sqft_share FROM stores;
