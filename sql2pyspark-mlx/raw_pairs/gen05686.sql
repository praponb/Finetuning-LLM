with recent_payments as (
    select *
    from payments
    where amount >= 800 or amount between 410 and 640
),
agg as (
    select method, concat_ws(', ', sort_array(collect_set(method))) as method_values, count(*) as cnt
    from recent_payments
    group by method
)
select method, method_values, cnt
from agg
where cnt > 7
order by cnt, method;
