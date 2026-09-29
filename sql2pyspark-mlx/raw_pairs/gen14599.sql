SELECT region FROM stores WHERE city IN ('Berlin', 'Bangkok', 'New York') UNION ALL SELECT region FROM stores WHERE sqft BETWEEN 6088 AND 12362 ORDER BY region DESC;
