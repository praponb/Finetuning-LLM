select a.id, a.product_id, a.units, a.sale_date from store_sales a where a.units > (select avg(b.units) from store_sales b where b.product_id = a.product_id);
