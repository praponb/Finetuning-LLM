SELECT id, region, city FROM stores WHERE region = 'West' OR region IN ('South', 'Central', 'West') ORDER BY city, id DESC;
