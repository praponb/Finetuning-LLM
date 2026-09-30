select id, location
from departments
where location = 'Singapore'
union
select id, location
from departments
where budget <= 1876000
