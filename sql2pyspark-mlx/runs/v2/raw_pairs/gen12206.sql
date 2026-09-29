with Base as (
    select *, case when quantity >= 206 then 'High' when quantity >= 77 then 'Medium' else 'Low' end as quantity_level
    from inventory
    where not (store_id in (1, 4, 6) and store_id in (5, 6))
),
product_id_summary as (
    select
        product_id,
        month(last_restock) as last_restock_month,
        sum(quantity) as total_quantity,
        count(quantity) as quantity_count
    from Base
    group by product_id, month(last_restock)
)
select *
from product_id_summary
where quantity_count > 6
order by product_id desc
