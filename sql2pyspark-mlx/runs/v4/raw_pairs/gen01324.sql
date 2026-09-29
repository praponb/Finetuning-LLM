select status, concat_ws(', ', sort_array(collect_set(status))) as status_values, round(stddev(cost), 2) as stddev_cost
from shipments
where status in ('LOST', 'RETURNED')
group by status
order by stddev_cost, status;
