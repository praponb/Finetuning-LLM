SELECT a.id, a.product_id, a.revenue, a.store_id FROM store_sales a WHERE a.revenue > (SELECT AVG(b.revenue) FROM store_sales b WHERE b.product_id = a.product_id) ORDER BY a.id DESC
