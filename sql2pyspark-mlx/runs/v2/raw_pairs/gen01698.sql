select id, supplier_id, price
from (
    select *, row_number() over (partition by supplier_id order by price desc, id) as row_num
    from products
) as t
where row_num = 1;
