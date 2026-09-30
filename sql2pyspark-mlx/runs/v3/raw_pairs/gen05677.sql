SELECT MAX(sqft) - MIN(sqft) AS sqft_range, MIN(opened_date) AS min_opened_date FROM stores WHERE region IS NOT NULL AND (sqft >= 15761 OR opened_date <= '2024-05-01');
