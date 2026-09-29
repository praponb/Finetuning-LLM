select a.id, a.product_id, a.revenue from store_sales a where a.revenue = (select max(b.revenue) from store_sales b where b.product_id = a.product_id) order by a.product_id desc;
