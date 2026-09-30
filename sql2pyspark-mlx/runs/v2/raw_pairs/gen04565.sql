with customer_stats as (
    select customer_id, count(*) as session_count, sum(case when browser = 'Chrome' then 1 else 0 end) as chrome_count
    from sessions
    where customer_id in (11, 17)
    group by customer_id
)
select
    c.name,
    c.country,
    cs.session_count,
    cs.chrome_count
from customers c
inner join customer_stats cs on c.id = cs.customer_id
order by cs.chrome_count;
