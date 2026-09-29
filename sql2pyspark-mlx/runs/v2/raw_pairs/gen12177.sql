SELECT id, city, store_name, sqft FROM stores WHERE region IN ('Central', 'West', 'South') AND (region NOT IN ('South', 'Central') OR region = 'South');
