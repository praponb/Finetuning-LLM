SELECT id, product_id, units, ROUND(units / revenue, 2) AS revenue_per_unit FROM store_sales ORDER BY id DESC
