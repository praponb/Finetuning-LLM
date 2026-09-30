select store_id, count(*) as total_count from store_sales group by store_id having count(*) >= 2 order by store_id desc, total_count desc;
