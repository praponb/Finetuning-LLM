with base as (
    select *
    from store_sales
    where sale_date >= '2025-10-01'
),
product_id_summary as (
    select product_id, min(sale_date) as first_sale_date
    from base
    group by product_id
)
select product_id, first_sale_date
from product_id_summary
