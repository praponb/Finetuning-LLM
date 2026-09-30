select location, sum(budget) as total_budget, count(*) as cnt, sum(sum(budget)) over (order by location) as cumulative_budget from departments group by location;
