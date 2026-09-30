with valid as (
    select *, lower(category) as category_lower
    from products
    where launch_date > date '2023-03-09'
)
select s.*
from valid s
where not exists (
        select 1
        from inventory x
        where x.product_id = s.id
          and x.quantity between 179 and 198
    );
