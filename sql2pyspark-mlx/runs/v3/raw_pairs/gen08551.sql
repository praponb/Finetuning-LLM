SELECT id, city, sqft, NTILE(10) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS quartile FROM stores ORDER BY quartile DESC
