select * from (select location, dept_name, budget from departments)
pivot (count(budget) for dept_name in ('Marketing', 'Legal', 'Engineering', 'Finance'));
