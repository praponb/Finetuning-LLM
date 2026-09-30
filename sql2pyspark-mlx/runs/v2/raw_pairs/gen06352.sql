select
    o.channel,
    o.store_id,
    round((100 * sum(case when p.status = 'SUCCESS' then 1 else 0 end)) / count(*), 1) as pct_success,
    avg(p.amount) as avg_amount
from payments p
inner join orders o on o.id = p.order_id
group by o.channel, o.store_id
order by store_id desc
