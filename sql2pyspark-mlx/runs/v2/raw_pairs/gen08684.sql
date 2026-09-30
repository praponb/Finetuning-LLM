SELECT region FROM stores WHERE region <> 'West' UNION ALL SELECT region FROM stores WHERE city <> 'Singapore';
