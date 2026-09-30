select
    case when year(paid_at) < 2024 then 'before_2024' when year(paid_at) = 2024 then '2024' else 'after_2024' end as paid_cohort,
    order_id,
    min(paid_at) as first_paid_at,
    sort_array(collect_set(status)) as status_list
from payments
where to_date(paid_at) < '2023-10-01'
group by case when year(paid_at) < 2024 then 'before_2024' when year(paid_at) = 2024 then '2024' else 'after_2024' end, order_id
order by paid_cohort
