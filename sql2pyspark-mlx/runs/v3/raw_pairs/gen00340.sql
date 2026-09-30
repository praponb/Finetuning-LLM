select id, brand, stock_qty
from (
    select *, row_number() over (partition by brand order by stock_qty desc, id) as row_num
    from products
) as t
where row_num <= 5;
