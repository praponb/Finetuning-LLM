select *
from (
    select *, row_number() over (partition by brand order by price desc, id) as rn
    from products
    where array_contains(tags, 'new')
) as ranked
where rn <= 1;
