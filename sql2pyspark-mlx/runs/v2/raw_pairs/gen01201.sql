select id, dept_name, budget, dense_rank() over (partition by dept_name order by budget desc) as dense_rnk from departments;
