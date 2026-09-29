select status, concat_ws(', ', sort_array(collect_set(status))) status_values, count(monthly_fee) monthly_fee_count
from subscriptions
where plan in ('premium', 'basic')
group by status
having count(monthly_fee) < 3
