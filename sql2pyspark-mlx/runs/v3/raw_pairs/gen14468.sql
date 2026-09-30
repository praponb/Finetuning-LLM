select product_id, avg(units) as avg_units from store_sales where revenue is not null group by 1 order by product_id desc;
