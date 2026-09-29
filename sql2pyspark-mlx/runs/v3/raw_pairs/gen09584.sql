select id, location, budget, round(avg(budget) over (partition by location order by budget, id rows between 2 preceding and current row), 2) as budget_moving_avg_3 from departments;
