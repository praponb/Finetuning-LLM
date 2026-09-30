select *
from (
    select
        case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end as delivered_cohort,
        year(delivered_date) as delivered_year,
        count(case when carrier = 'Kerry' then 1 end) as kerry_count,
        sum(cost) as total_cost
    from shipments
    where cost > 39
    group by case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end, year(delivered_date)
) as sub
where kerry_count > 9
order by delivered_year asc;
