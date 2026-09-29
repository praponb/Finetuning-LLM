select status, sum(case when channel = 'web' then 1 else 0 end) as web_count, sort_array(collect_set(status)) as status_list from orders where amount >= 640 group by status order by status;
