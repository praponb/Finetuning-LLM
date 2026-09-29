select *, lag(budget) over w_recent as prev_budget, row_number() over w_recent as seq_no from departments window w_recent as (partition by dept_name order by budget, id);
