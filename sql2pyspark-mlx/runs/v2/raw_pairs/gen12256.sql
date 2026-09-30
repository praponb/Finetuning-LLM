select order_id, status, sort_array(collect_set(status)) as status_list
from shipments
group by order_id, status;
