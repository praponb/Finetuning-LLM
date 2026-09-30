with customer_totals as (
    select customer_id, concat_ws(', ', sort_array(collect_set(country))) as country_values
    from sessions
    group by customer_id
)
select c.id, c.age, ct.country_values
from customers c
left join customer_totals ct on c.id = ct.customer_id
