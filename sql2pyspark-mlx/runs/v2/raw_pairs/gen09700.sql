SELECT units, product_id, COALESCE(units, 0) AS units_filled, units - revenue AS revenue_per_unit FROM store_sales ORDER BY 3 DESC;
