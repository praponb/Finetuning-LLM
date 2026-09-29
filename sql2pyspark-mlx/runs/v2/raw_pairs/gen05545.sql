with Src as (
    select *, case when rating >= 4.1 then 'High' when rating >= 2.1 then 'Medium' else 'Low' end as rating_band
    from suppliers
    where country in ('JP', 'SG')
),
CitySummary as (
    select city, sum(case when city = 'Tokyo' then 1 else 0 end) as tokyo_count, round((100 * sum(case when country = 'SG' then 1 else 0 end)) / count(*), 1) as pct_sg
    from Src
    group by city
)
select city, tokyo_count
from CitySummary
where pct_sg >= 65
order by tokyo_count, city;
