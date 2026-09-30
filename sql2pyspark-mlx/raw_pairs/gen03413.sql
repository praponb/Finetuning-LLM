select *, lead(budget) over (partition by location, dept_name order by budget, id) as next_budget, row_number() over (partition by location, dept_name order by budget, id) as seq_no from departments
