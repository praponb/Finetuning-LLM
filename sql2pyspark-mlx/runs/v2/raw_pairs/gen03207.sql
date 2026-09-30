select a.opened_date, c.signup_date
from accounts a
inner join customers c on a.customer_id = c.id
where c.country = 'DE'
order by opened_date, c.signup_date;
