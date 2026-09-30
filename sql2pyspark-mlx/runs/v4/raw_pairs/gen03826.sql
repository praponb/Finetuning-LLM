select s.id, s.product_id, s.revenue, s.store_id from store_sales s where s.revenue < (select avg(s2.revenue) from store_sales s2 where s2.product_id = s.product_id) order by s.product_id desc
