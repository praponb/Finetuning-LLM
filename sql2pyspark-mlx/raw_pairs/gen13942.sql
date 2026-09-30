select
    c.segment,
    c.city,
    max(we.duration_sec) as max_duration_sec,
    max(we.duration_sec) - min(we.duration_sec) as duration_sec_range
from web_events we
inner join sessions s on s.id = we.session_id
full outer join customers c on s.customer_id = c.id
group by c.segment, c.city
order by duration_sec_range asc;
