with top_suppliers as (
    select
        id,
        country,
        rating,
        supplier_name,
        row_number() over (partition by country order by rating desc, id) as pos
    from suppliers
)
select *
from top_suppliers
where pos <= 3
