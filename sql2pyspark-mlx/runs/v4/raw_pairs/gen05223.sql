with sessions_by_customer as (
    select customer_id, count(case when device = 'desktop' then 1 end) as desktop_count, count(distinct country) as unique_countrys
    from sessions
    group by customer_id
)
select
    c.id,
    c.email,
    c.name,
    sbc.desktop_count,
    sbc.unique_countrys
from customers c
inner join sessions_by_customer sbc on c.id = sbc.customer_id
where sbc.desktop_count > 7
