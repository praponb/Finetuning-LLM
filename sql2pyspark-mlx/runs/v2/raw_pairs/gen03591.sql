select id, email
from customers
where length(name) > 9
union
select id, email
from customers
where signup_date >= '2023-10-04';
