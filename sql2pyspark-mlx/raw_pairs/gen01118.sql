select
    status,
    case when year(shipped_date) < 2024 then 'before_2024' when year(shipped_date) = 2024 then '2024' else 'after_2024' end as shipped_cohort,
    min(cost) as min_cost,
    concat_ws(', ', sort_array(collect_set(carrier))) as carrier_values
from shipments
where shipped_date >= '2025-06-01'
group by 1, 2
having min(cost) > 51
