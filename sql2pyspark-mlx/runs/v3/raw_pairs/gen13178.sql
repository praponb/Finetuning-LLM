select case when year(end_date) < 2024 then 'before_2024' when year(end_date) = 2024 then '2024' else 'after_2024' end as end_cohort, sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_count, concat_ws(', ', sort_array(collect_set(plan))) as plan_values
from subscriptions
where monthly_fee is null
group by end_cohort
order by end_cohort desc
limit 50;
