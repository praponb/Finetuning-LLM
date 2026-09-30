select date_format(s.started_at, 'yyyy-MM') as started_month, count(case when we.event_type = 'add_to_cart' then 1 end) as add_to_cart_count, max(we.duration_sec) - min(we.duration_sec) as duration_sec_range
from web_events we
join sessions s on we.session_id = s.id
where we.event_time >= '2025-04-22'
group by date_format(s.started_at, 'yyyy-MM');
