with customer_totals as (
    select customer_id, sum(case when plan = 'premium' then 1 else 0 end) as premium_count, round(avg(monthly_fee), 2) as avg_monthly_fee
    from subscriptions
    group by customer_id
)
select c.email, ct.premium_count, ct.avg_monthly_fee
from customers c
join customer_totals ct on c.id = ct.customer_id
order by ct.avg_monthly_fee desc;
