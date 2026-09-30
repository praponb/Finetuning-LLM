select o.channel, round((100 * sum(case when p.status = 'FAILED' then 1 else 0 end)) / count(*), 1) as pct_failed
from payments p
join orders o on p.order_id = o.id
where o.channel = 'mobile'
group by o.channel
having round((100 * sum(case when p.status = 'FAILED' then 1 else 0 end)) / count(*), 1) >= 76
order by o.channel;
