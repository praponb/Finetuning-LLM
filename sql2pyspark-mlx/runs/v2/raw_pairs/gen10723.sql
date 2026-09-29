SELECT id, units, COALESCE(revenue, 0) AS revenue_filled FROM store_sales WHERE revenue BETWEEN 2300 AND 3100 AND units > 40
