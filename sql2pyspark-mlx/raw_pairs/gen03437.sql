select id, browser, customer_id
from sessions
where started_at >= '2025-04-16'
union
select id, browser, customer_id
from sessions
where country = 'TH'
order by customer_id desc;
