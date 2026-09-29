select quarter(customers.signup_date) as signup_quarter, max(sessions.started_at) as last_started_at, sum(case when sessions.device = 'mobile' then 1 else 0 end) as mobile_count
from sessions
left join customers on sessions.customer_id = customers.id
group by quarter(customers.signup_date)
order by signup_quarter desc;
