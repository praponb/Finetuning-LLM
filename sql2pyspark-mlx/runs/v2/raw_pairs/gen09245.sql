with base as (
    select *
    from payments
    where status = 'FAILED' and (hour(paid_at) between 7 and 15 or method = 'cash')
),
method_summary as (
    select
        method,
        order_id,
        concat_ws(', ', sort_array(collect_set(method))) as method_values,
        count(case when status = 'SUCCESS' then 1 end) as success_count
    from base
    group by method, order_id
)
select *
from method_summary
where success_count < 7
order by method desc, success_count asc;
