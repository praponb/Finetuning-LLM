with CustomerStats as (
    select customer_id, sum(case when status = 'FROZEN' then 1 else 0 end) as frozen_count
    from accounts
    where opened_date between '2025-01-01' and '2025-06-01'
    group by customer_id
)
select
    c.id,
    c.age,
    c.credit_limit,
    C.frozen_count
from customers c
join CustomerStats C on c.id = C.customer_id
where C.frozen_count >= 26
order by C.frozen_count desc;
