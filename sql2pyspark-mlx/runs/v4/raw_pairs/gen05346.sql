select id, budget, row_number() over (order by budget desc, id) as rn
from departments;
