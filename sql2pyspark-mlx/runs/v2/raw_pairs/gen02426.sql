select
    s.id,
    s.started_at,
    s.country,
    s.browser
from sessions s
where s.device = 'tablet' and s.customer_id in (select id from customers where country != 'SG')
order by started_at
