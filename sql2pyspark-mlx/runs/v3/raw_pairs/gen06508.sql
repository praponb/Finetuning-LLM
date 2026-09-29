with SessionsByCustomer as (
    select customer_id, sum(case when device = 'mobile' then 1 else 0 end) mobile_count, min(started_at) first_started_at
    from sessions
    where customer_id = 34
    group by customer_id
)
select c.segment, S.mobile_count, S.first_started_at
from customers c
join SessionsByCustomer S on c.id = S.customer_id
where S.mobile_count > 61;
