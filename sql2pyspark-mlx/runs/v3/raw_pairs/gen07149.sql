with customer_totals as (
    select customer_id, sort_array(collect_set(plan)) as plan_list, max(monthly_fee) - min(monthly_fee) as monthly_fee_range
    from subscriptions
    group by customer_id
)
select
    customers.id,
    customers.age,
    customers.is_active,
    customer_totals.plan_list,
    customer_totals.monthly_fee_range
from customers
inner join customer_totals on customers.id = customer_totals.customer_id
