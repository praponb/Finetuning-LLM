with customer_summary as (
    select customer_id, min(monthly_fee) min_monthly_fee, count(distinct monthly_fee) unique_statuss
    from subscriptions
    group by customer_id
)
select
    customers.credit_limit,
    customers.age,
    customer_summary.min_monthly_fee,
    customer_summary.unique_statuss
from customers
left join customer_summary on customers.id = customer_summary.customer_id
where customer_summary.min_monthly_fee >= 210
