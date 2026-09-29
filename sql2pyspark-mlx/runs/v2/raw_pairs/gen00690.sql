select
    quarter(o.order_date) as order_quarter,
    date_trunc('MONTH', o.order_date) as order_month,
    count(distinct p.amount) as unique_orders,
    round((100 * sum(case when p.status = 'SUCCESS' then 1 else 0 end)) / count(*), 1) as pct_success
from payments p
inner join orders o on p.order_id = o.id
group by quarter(o.order_date), date_trunc('MONTH', o.order_date)
order by unique_orders desc;
