with recent_shipments as (
    select *
    from shipments
    where shipped_date > '2023-06-01'
),
delivered_cohort_summary as (
    select case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end as delivered_cohort, max(delivered_date) as latest_delivered_date
    from recent_shipments
    group by case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end
)
select *
from delivered_cohort_summary
order by delivered_cohort desc;
