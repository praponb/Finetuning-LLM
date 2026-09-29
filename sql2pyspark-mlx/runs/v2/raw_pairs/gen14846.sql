with numbered as (
    select
        id,
        supplier_id,
        launch_date,
        category,
        rank() over (partition by supplier_id order by launch_date) as rn
    from products
)
select
    id,
    supplier_id,
    launch_date,
    category
from numbered
where rn <= 3;
