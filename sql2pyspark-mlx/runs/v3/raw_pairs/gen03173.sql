select date_trunc('MONTH', c.signup_date) signup_month, count(*) web_event_count
from web_events we
right outer join sessions s on s.id = we.session_id
full outer join customers c on s.customer_id = c.id
group by date_trunc('MONTH', c.signup_date);
