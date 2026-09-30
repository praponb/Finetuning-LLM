with numbered as (
    select
        id,
        brand,
        price,
        rank() over (partition by brand order by price desc) as pos
    from products
    where cost <= 680
)
select id, brand, price
from numbered
where pos <= 3
order by brand;
