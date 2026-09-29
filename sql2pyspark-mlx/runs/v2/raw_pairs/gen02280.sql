select id, store_id, sale_date
from (
    select *, row_number() over (partition by store_id order by sale_date desc, id) as pos
    from store_sales
    where product_id in (2, 3, 4)
) as r
where pos = 1
order by store_id desc;
