select level, is_remote, stddev_salary
from (
    select level, is_remote, round(stddev(salary), 2) stddev_salary
    from employees
    group by level, is_remote
) s
where stddev_salary < 42
