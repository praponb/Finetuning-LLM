select
    status,
    date_format(start_date, 'yyyy-MM') start_month,
    date_trunc('MONTH', end_date) end_month,
    concat_ws(', ', sort_array(collect_set(plan))) plan_values
from subscriptions
where month(start_date) in (2, 3, 7)
group by status, date_format(start_date, 'yyyy-MM'), date_trunc('MONTH', end_date);
