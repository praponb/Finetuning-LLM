with ranked_suppliers as (
    select *, row_number() over (partition by country order by rating desc, id) as pos
    from suppliers
)
select
    id,
    country,
    rating,
    supplier_name
from ranked_suppliers
where pos <= 5
order by country desc
