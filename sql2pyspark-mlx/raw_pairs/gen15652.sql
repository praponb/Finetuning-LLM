select
    s.id,
    s.country,
    s.browser,
    s.started_at
from sessions s
where s.browser in ('Safari', 'Edge', 'Firefox') and s.customer_id in (select id from customers where city = 'Singapore')
order by browser
