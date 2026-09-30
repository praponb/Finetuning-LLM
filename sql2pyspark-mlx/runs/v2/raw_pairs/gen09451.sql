SELECT city, sqft, opened_date FROM stores WHERE region IN ('East', 'West', 'North') AND (sqft = 14630 OR region IS NOT NULL)
