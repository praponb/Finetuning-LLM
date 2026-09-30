select id, location, budget, rank() over (partition by location order by budget desc) as budget_rank, round(budget / sum(budget) over (partition by location), 3) as budget_share from departments;
