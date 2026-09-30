select concat_ws(', ', sort_array(collect_set(carrier))) carrier_values, round((100 * sum(case when carrier = 'Flash' then 1 else 0 end)) / count(*), 1) pct_flash
from shipments
where status = 'LOST';
