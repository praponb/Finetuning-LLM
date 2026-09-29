SELECT opened_date, sqft FROM stores WHERE sqft BETWEEN 7812 AND 15040 UNION SELECT opened_date, sqft FROM stores WHERE opened_date <= '2025-03-01'
