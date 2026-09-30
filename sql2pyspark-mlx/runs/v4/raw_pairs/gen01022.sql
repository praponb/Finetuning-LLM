select * from (select job_title, count(distinct salary) as unique_managers, max(salary) - min(salary) as salary_range from employees group by job_title) as x where unique_managers >= 8
