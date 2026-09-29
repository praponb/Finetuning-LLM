select
    a.*,
    sub.id as subscription_id,
    sub.plan,
    sub.monthly_fee
from orders a
inner join subscriptions sub
    on a.customer_id = sub.customer_id
   and a.order_date >= sub.start_date
   and a.order_date < sub.end_date + interval '7' day;
