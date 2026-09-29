select * from (select store_id, count(distinct revenue) as unique_stores from store_sales group by store_id) as agg where unique_stores > 5 order by 1 desc
