select status, sum(amount) total_amount, concat_ws(', ', sort_array(collect_set(status))) status_values
from payments
group by status
having sum(amount) >= 8600
order by status_values desc, 2, status
limit 5
