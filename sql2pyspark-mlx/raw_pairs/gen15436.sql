select s.browser, count(we.id) as web_event_count, sum(case when we.event_type = 'purchase' then 1 else 0 end) as purchase_count
from sessions s
join web_events we on s.id = we.session_id
where s.device = 'mobile'
group by s.browser
having sum(case when we.event_type = 'purchase' then 1 else 0 end) >= 70
order by 3 desc, browser
limit 5
