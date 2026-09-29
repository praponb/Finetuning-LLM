with numbered as (
    select *, row_number() over (partition by city order by rating, id) as row_num
    from suppliers
)
select
    id,
    city,
    rating,
    supplier_name
from numbered
where row_num <= 1
order by id desc
