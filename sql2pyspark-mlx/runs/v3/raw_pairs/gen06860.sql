with numbered as (
    select
        id,
        supplier_id,
        launch_date,
        brand,
        row_number() over (partition by supplier_id order by launch_date desc, id) as row_num
    from products
)
select
    id,
    supplier_id,
    launch_date,
    brand
from numbered
where row_num <= 3
order by supplier_id desc;
