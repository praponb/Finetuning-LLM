with top_products as (
    select *, row_number() over (partition by category order by cost desc, id) as rn
    from products
)
select *
from top_products
where rn <= 5;
