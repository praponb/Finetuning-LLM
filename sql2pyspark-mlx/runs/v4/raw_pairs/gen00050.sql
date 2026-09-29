SELECT id, sqft, region FROM stores WHERE sqft > (SELECT MAX(sqft) FROM stores WHERE region IN ('North', 'South', 'Central')) ORDER BY id, region
