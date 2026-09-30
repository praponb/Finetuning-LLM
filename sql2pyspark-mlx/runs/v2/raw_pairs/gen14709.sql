SELECT region, west_count FROM (SELECT region, SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) AS west_count FROM stores WHERE region IS NULL GROUP BY region) AS x WHERE west_count >= 49
