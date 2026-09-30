SELECT ss.id, ss.revenue, p.cost, p.price FROM store_sales ss RIGHT OUTER JOIN products p ON ss.product_id = p.id ORDER BY revenue DESC
