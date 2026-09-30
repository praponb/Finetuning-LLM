select id, age
from customers
where segment in ('Corporate', 'Consumer')
except
select id, age
from customers
where age is not null;
