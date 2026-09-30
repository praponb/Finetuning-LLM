select
    c.id,
    c.segment,
    count(s.id) as subscription_count,
    round(stddev(s.monthly_fee), 2) as stddev_monthly_fee,
    min(s.monthly_fee) as min_monthly_fee
from customers c
left join subscriptions s on s.customer_id = c.id
group by c.id, c.segment
order by id desc, min_monthly_fee;
