select quarter(delivered_date) as delivered_quarter, concat_ws(', ', sort_array(collect_set(carrier))) as carrier_values, count(cost) as cost_count
from shipments
group by quarter(delivered_date)
