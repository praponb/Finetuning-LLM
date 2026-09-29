select id, product_id, units
from (
    select
        id,
        product_id,
        units,
        row_number() over (partition by product_id order by units desc, id) as rnk
    from store_sales
) as t
where rnk <= 3;
