SELECT id, rating, CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg FROM suppliers WHERE city IN ('London', 'Bangkok') OR country = 'US';
