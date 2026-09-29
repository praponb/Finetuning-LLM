select we.page, s.device, s.customer_id
from web_events we
left join sessions s on s.id = we.session_id
where s.browser in ('Firefox', 'Chrome', 'Edge') and s.device = 'desktop'
order by s.device desc;
