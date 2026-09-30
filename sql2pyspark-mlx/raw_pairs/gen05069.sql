select
    s.id,
    s.name,
    s.country,
    s.city,
    s.is_active,
    s.credit_limit,
    s.age,
    latest.session_started_at
from customers s
left join lateral (
        select c.started_at as session_started_at
        from sessions c
        where c.customer_id = s.id
        order by c.started_at desc, c.id desc
        limit 1
    ) latest on true;
