select
    opened_cohort,
    region,
    chiang_mai_count,
    sqft_count
from (
    select
        case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
        region,
        count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count,
        count(sqft) as sqft_count
    from stores
    group by case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end, region
) as t
where sqft_count > 3;
