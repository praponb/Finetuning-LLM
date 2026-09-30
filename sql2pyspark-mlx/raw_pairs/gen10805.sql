with customer_summary as (
    select customer_id, round((100 * sum(case when plan = 'premium' then 1 else 0 end)) / count(*), 1) as pct_premium, sum(monthly_fee) as total_monthly_fee
    from subscriptions
    where status in ('CANCELLED', 'ACTIVE')
    group by customer_id
)
select c.city, cs.pct_premium, cs.total_monthly_fee
from customers c
inner join customer_summary cs on c.id = cs.customer_id
where cs.total_monthly_fee > 480;
