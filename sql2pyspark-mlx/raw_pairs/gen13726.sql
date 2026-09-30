select a.id, a.product_id, a.revenue from store_sales a where a.revenue > (select avg(b.revenue) from store_sales b where b.product_id = a.product_id)
