select min(salary) as min_salary, sort_array(collect_set(level)) as level_list
from employees
