select
    c.id,
    c.signup_date,
    t.cancelled_count,
    t.plan_list
from customers c
inner join (select customer_id, sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_count, sort_array(collect_set(plan)) as plan_list
    from subscriptions
    group by customer_id) t on c.id = t.customer_id
order by 2, id desc
limit 3;
