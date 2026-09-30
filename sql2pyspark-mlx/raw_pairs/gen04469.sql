select
    c.id,
    c.city,
    c.signup_date,
    t.frozen_count
from customers c
left join (select customer_id, sum(case when status = 'FROZEN' then 1 else 0 end) frozen_count
    from accounts
    group by customer_id) t on c.id = t.customer_id
where t.frozen_count >= 75
order by id desc
