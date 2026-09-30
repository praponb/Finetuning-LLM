select segment, datediff(current_date(), signup_date) as days_since_signup_date
from customers
where email like '%@company.co'
order by segment desc, days_since_signup_date desc
