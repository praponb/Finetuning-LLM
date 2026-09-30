select c.age, c.signup_date, a.opened_date
from customers c
left join accounts a on a.customer_id = c.id
where a.account_type = 'CHECKING'
order by opened_date desc, age;
