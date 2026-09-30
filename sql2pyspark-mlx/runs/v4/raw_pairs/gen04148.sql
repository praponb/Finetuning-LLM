with ranked as (
    select *, row_number() over (partition by store_id order by sale_date desc, id) as pos
    from store_sales
    where units = 33
)
select
    id,
    store_id,
    sale_date,
    product_id
from ranked
where pos <= 2
order by sale_date, store_id desc
