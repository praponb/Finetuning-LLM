SELECT sqft, opened_date FROM stores WHERE city IN ('Chiang Mai', 'Bangkok') UNION ALL SELECT sqft, opened_date FROM stores WHERE opened_date < '2025-05-01' ORDER BY opened_date;
