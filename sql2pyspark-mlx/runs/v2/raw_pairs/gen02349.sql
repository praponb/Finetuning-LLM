SELECT id, region, CASE WHEN region = 'Central' THEN 1 ELSE 0 END AS is_central FROM stores WHERE region IS NOT NULL
