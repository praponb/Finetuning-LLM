select * from (select location, dept_name, budget from departments where location in ('Berlin', 'Singapore'))
pivot (max(budget) for dept_name in ('Legal', 'Marketing'))
order by location;
