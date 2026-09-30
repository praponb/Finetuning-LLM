SELECT product_id, CEIL(revenue) AS revenue_whole, GREATEST(revenue, units) AS greatest_revenue_units FROM store_sales;
