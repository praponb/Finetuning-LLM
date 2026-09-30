SELECT s.id, s.store_id, s.revenue, s.units FROM store_sales s WHERE s.revenue < (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.store_id = s.store_id)
