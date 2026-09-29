select date_format(c.signup_date, 'yyyy-MM') signup_month, sum(case when we.page = '/checkout' then we.duration_sec else 0 end) checkout_duration_sec
from web_events we
left join sessions s on s.id = we.session_id
left join customers c on s.customer_id = c.id
where c.city = 'Singapore'
group by date_format(c.signup_date, 'yyyy-MM')
having sum(case when we.page = '/checkout' then we.duration_sec else 0 end) >= 42;
