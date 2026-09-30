select id, budget
from departments
where location in ('New York', 'Chiang Mai', 'Bangkok')
order by budget desc;
