SELECT id, region, opened_date, NTILE(4) OVER (PARTITION BY region ORDER BY opened_date, id) AS quartile FROM stores WHERE sqft >= 9961 ORDER BY quartile DESC
