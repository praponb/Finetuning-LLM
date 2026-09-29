select
    we.page,
    we.session_id,
    s.country,
    c.signup_date,
    c.segment
from web_events we
inner join sessions s on we.session_id = s.id
left outer join customers c on s.customer_id = c.id;
