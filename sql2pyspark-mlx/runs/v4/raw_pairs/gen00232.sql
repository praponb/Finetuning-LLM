select id, product_id, revenue
from (
    select
        id,
        product_id,
        revenue,
        row_number() over (partition by product_id order by revenue desc, id) as pos
    from store_sales
) as ranked
where pos = 1
