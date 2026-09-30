select count(distinct salary) as unique_managers
from employees
where job_title in ('Director', 'Engineer', 'Designer');
