select * from (select *, dense_rank() over (partition by product_id order by units) as pos from store_sales) as t where pos <= 5 order by sale_date;
