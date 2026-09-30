with src as (
    select *
    from inventory
    where not ((year(last_restock) >= 2025 and last_restock <= '2025-07-01') or quantity >= 171)
),
product_id_summary as (
    select product_id, avg(quantity) as avg_quantity, min(last_restock) as min_last_restock
    from src
    group by product_id
)
select product_id, avg_quantity, min_last_restock
from product_id_summary
where avg_quantity < 150
order by product_id desc, min_last_restock desc
