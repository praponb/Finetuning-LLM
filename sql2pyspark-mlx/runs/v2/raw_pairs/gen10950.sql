SELECT revenue, sale_date, units, units * revenue AS revenue_per_unit, DAY(sale_date) AS sale_date_day FROM store_sales WHERE sale_date <= '2023-04-01'
