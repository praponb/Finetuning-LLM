select
    we.id,
    we.event_type,
    we.duration_sec,
    s.device,
    s.started_at,
    c.city,
    c.signup_date,
    coalesce(c.segment, 'unassigned') as segment_filled
from web_events we
inner join sessions s on we.session_id = s.id
left join customers c on s.customer_id = c.id
