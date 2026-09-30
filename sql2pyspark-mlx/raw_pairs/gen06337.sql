select id, budget, rank() over (order by budget) as budget_rank from departments
