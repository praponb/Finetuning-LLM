with ranked_stores as (
    select
        id,
        region,
        opened_date,
        city,
        rank() over (partition by region order by opened_date desc) as row_num
    from stores
)
select *
from ranked_stores
where row_num <= 2
order by city, opened_date;
