select
    c.segment,
    year(c.signup_date) signup_year,
    max(s.started_at) last_started_at,
    count(*) num_sessions
from sessions s
join customers c on s.customer_id = c.id
group by c.segment, year(c.signup_date)
order by num_sessions desc, signup_year desc;
