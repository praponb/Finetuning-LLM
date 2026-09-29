select * from (select status, count(cost) as cost_count from shipments where status <> 'DELIVERED' group by status) as agg where cost_count < 9 order by cost_count desc, status limit 10
