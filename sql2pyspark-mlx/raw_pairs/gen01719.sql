with numbered as (
    select
        id,
        brand,
        stock_qty,
        price,
        row_number() over (partition by brand order by stock_qty desc, id) as rn
    from products
)
select
    id,
    brand,
    stock_qty,
    price
from numbered
where rn = 1;
