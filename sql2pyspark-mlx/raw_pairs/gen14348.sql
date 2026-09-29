select country as label
from customers
union
select location as label
from departments;
