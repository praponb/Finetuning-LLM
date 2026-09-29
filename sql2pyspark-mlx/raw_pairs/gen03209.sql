SELECT region, city FROM stores WHERE opened_date > '2025-08-01' EXCEPT SELECT region, city FROM stores WHERE sqft <= 11668;
