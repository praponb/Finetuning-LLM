with RankedProducts as (
    select
        id,
        brand,
        launch_date,
        dense_rank() over (partition by brand order by launch_date desc) as pos
    from products
)
select id, brand, launch_date
from RankedProducts
where pos = 1
order by launch_date asc;
