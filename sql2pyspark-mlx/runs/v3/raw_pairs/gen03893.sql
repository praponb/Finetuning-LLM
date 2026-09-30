select id, budget from departments where budget >= (select max(budget) from departments) order by budget
