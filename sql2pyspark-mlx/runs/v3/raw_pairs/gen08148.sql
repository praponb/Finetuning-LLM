select s.id, s.product_id, s.units, s.sale_date from store_sales s where s.units > (select avg(s2.units) from store_sales s2 where s2.product_id = s.product_id);
