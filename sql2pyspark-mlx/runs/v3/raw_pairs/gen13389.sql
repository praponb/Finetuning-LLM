select
    c.id,
    c.city,
    count(o.id) as order_count,
    round((100 * sum(case when o.channel = 'web' then 1 else 0 end)) / count(*), 1) as pct_web
from customers c
left join orders o on c.id = o.customer_id
where c.signup_date > '2025-04-01'
group by c.id, c.city
order by order_count;
