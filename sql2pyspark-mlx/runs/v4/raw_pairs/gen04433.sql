select *
from (
    select
        id,
        category,
        stock_qty,
        row_number() over (partition by category order by stock_qty desc, id) as row_num
    from products
) as t
where row_num = 1
order by stock_qty desc
