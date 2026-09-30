SELECT id, product_id, revenue, units, YEAR(sale_date) AS sale_date_year, units - revenue AS revenue_per_unit FROM store_sales WHERE revenue <= 1300 AND product_id = 3 ORDER BY revenue_per_unit
