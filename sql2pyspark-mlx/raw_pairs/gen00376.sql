SELECT region, COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count, COUNT(DISTINCT sqft) AS unique_regions FROM stores GROUP BY region ORDER BY unique_regions DESC, region;
