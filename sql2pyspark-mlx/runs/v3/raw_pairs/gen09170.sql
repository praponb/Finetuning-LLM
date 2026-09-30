select a.balance, c.signup_date
from accounts a
join customers c on c.id = a.customer_id
order by signup_date;
