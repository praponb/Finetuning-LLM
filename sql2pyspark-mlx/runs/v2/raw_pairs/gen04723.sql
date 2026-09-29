with customer_stats as (
    select customer_id, count(case when country = 'JP' then 1 end) as jp_count
    from sessions
    where browser not in ('Safari', 'Firefox')
    group by customer_id
)
select c.id, c.credit_limit, cs.jp_count
from customers c
left join customer_stats cs on c.id = cs.customer_id
order by id;
