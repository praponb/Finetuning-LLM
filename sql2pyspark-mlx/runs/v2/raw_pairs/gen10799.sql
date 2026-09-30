select
    s.id,
    s.browser,
    count(we.id) as web_event_count,
    sum(case when we.event_type = 'click' then 1 else 0 end) as click_count
from sessions s
left join web_events we on s.id = we.session_id
where we.page not in ('/product', '/search', '/home')
group by s.id, s.browser;
