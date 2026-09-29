SELECT COUNT(DISTINCT units) AS unique_products, SUM(units) AS total_units FROM store_sales WHERE revenue <= 1400 ORDER BY total_units DESC
