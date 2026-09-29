with order_stats as (
    select order_id, round((100 * sum(case when status = 'FAILED' then 1 else 0 end)) / count(*), 1) as pct_failed
    from payments
    group by order_id
)
select o.id, o.shipping_fee, os.pct_failed
from orders o
left join order_stats os on o.id = os.order_id
where os.pct_failed >= 31;
