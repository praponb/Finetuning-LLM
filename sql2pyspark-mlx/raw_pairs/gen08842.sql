select
    *,
    count(budget) over w_part as last7_count_budget,
    max(budget) over w_part as last7_max_budget,
    avg(budget) over w_part as last7_avg_budget
from departments
where dept_name <> 'Finance'
window w_part as (partition by dept_name order by budget, id rows between 6 preceding and current row)
