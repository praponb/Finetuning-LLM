select segment, count(*) total_count, max(signup_date) latest_signup_date
from customers
group by segment
order by total_count desc, segment
limit 10
