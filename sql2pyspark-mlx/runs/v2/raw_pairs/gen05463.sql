SELECT a.id, a.product_id, a.revenue, a.sale_date FROM store_sales a WHERE a.revenue = (SELECT MAX(b.revenue) FROM store_sales b WHERE b.product_id = a.product_id)
