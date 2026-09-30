select
    we.event_time,
    s.browser,
    s.country,
    c.city,
    quarter(s.started_at) as started_at_quarter
from web_events we
join sessions s on s.id = we.session_id
left join customers c on s.customer_id = c.id
order by 4
