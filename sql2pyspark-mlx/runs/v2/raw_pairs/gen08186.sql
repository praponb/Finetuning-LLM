with ranked_suppliers as (
    select *, row_number() over (partition by city order by rating desc, id) as pos
    from suppliers
)
select
    id,
    city,
    rating,
    supplier_name
from ranked_suppliers
where pos <= 3
order by city asc;
