select *, length(dept_name) dept_name_length
from departments
where location is not null or dept_name = 'Engineering';
