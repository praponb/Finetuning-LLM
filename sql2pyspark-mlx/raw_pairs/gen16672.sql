select s.browser, count(*) as num_web_events
from web_events we
inner join sessions s on we.session_id = s.id
where s.device in ('mobile', 'desktop')
group by s.browser
order by browser, num_web_events desc;
