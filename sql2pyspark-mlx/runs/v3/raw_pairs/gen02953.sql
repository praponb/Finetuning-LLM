select date_format(c.signup_date, 'yyyy-MM') as signup_month, c.is_active, max(a.opened_date) as latest_opened_date
from accounts a
join customers c on a.customer_id = c.id
group by date_format(c.signup_date, 'yyyy-MM'), c.is_active
order by signup_month desc;
