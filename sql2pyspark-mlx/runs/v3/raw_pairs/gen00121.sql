with ranked_suppliers as (
    select *, row_number() over (partition by country order by rating desc, id) as pos
    from suppliers
    where city = 'Bangkok'
)
select *
from ranked_suppliers
where pos <= 3
order by pos desc, supplier_name
