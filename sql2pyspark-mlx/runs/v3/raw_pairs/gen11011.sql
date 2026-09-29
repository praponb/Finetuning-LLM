with numbered as (
    select
        id,
        region,
        opened_date,
        store_name,
        row_number() over (partition by region order by opened_date desc, id) as row_num
    from stores
    where city = 'London'
)
select
    id,
    region,
    opened_date,
    store_name
from numbered
where row_num <= 3
order by 1, opened_date desc
