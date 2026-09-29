select id, brand, stock_qty
from (
    select *, row_number() over (partition by brand order by stock_qty, id) as row_num
    from products
) as ranked
where row_num = 1;
