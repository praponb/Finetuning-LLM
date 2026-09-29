SELECT id, opened_date FROM stores WHERE region <> 'West' EXCEPT SELECT id, opened_date FROM stores WHERE sqft > 8633 ORDER BY id
