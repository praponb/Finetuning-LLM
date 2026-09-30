with base as (
    select *
    from suppliers
    where rating >= 3.0 and country in ('US', 'TH')
),
city_summary as (
    select city, count(case when country = 'TH' then 1 end) as th_count
    from base
    group by city
)
select *
from city_summary
where th_count > 9
order by city, th_count desc;
