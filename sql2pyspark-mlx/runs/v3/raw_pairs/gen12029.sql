SELECT id, revenue, product_id, units FROM store_sales WHERE revenue > (SELECT MAX(revenue) FROM store_sales) ORDER BY revenue DESC, id LIMIT 50;
