select
    dim.status as order_status,
    e.method,
    count(*) as rows_total,
    round(avg(e.amount), 2) as avg_amount,
    grouping_id() as level_id
from payments e
inner join orders dim on e.order_id = dim.id
group by grouping sets ((dim.status, e.method), (dim.status))
order by order_status desc nulls first;
