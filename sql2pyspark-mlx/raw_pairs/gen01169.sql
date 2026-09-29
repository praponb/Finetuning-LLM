select * from (select location, dept_name, budget from departments) src pivot (sum(budget) as value_sum for dept_name in ('HR' as hr, 'Sales' as sales))
