SELECT region FROM stores WHERE region IN ('North', 'West', 'Central') UNION SELECT region FROM stores WHERE sqft <= 12555 ORDER BY region DESC;
