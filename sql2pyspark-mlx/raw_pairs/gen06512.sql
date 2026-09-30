select paid_month, method_values, success_count
from (
    select month(paid_at) as paid_month, concat_ws(', ', sort_array(collect_set(method))) as method_values, sum(case when status = 'SUCCESS' then 1 else 0 end) as success_count
    from payments
    group by month(paid_at)
) as agg
where success_count < 51
order by paid_month desc
