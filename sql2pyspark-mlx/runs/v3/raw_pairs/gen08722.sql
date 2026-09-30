select s.country, max(we.duration_sec) as max_duration_sec
from web_events we
left join sessions s on s.id = we.session_id
inner join customers c on s.customer_id = c.id
where c.email like '%@company.co'
group by s.country
order by max_duration_sec
