select id, status, cost, shipped_date from (select *, dense_rank() over (partition by status order by cost) as row_num from shipments) as t where row_num <= 3 order by shipped_date asc
