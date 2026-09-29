with latest as (
    select *
    from inventory
    qualify row_number() over (partition by store_id, product_id order by quantity nulls last, id) = 1
)
select
    date_trunc('month', last_restock) as activity_month,
    stddev_samp(quantity) as quantity_stddev,
    max(quantity) as max_quantity
from latest
group by date_trunc('month', last_restock)
order by activity_month desc nulls last
