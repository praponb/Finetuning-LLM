select c.segment, month(c.signup_date) signup_month, count(*) account_count
from accounts a
left outer join customers c on a.customer_id = c.id
group by c.segment, month(c.signup_date);
