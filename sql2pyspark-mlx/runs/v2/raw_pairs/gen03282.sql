with customer_summary as (
    select customer_id, sum(case when device = 'tablet' then 1 else 0 end) as tablet_count
    from sessions
    group by customer_id
)
select
    c.id,
    c.country,
    c.credit_limit,
    cs.tablet_count
from customers c
left join customer_summary cs on c.id = cs.customer_id
where cs.tablet_count >= 41
order by c.credit_limit asc;
