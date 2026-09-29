select year(c.signup_date) as signup_year, min(s.started_at) as min_started_at, count(distinct s.browser) as unique_browsers
from sessions s
inner join customers c on s.customer_id = c.id
group by year(c.signup_date)
order by signup_year asc;
