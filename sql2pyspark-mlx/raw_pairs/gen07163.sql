SELECT *, units + revenue AS revenue_per_unit, ROUND(revenue * 1.1, 2) AS revenue_adjusted FROM store_sales WHERE NOT (YEAR(sale_date) >= 2023 OR units <= 39) ORDER BY sale_date DESC, id LIMIT 10
