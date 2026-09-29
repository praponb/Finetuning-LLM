with TopSuppliers as (
    select *, row_number() over (partition by country order by rating desc, id) pos
    from suppliers
)
select *
from TopSuppliers
where pos <= 5;
