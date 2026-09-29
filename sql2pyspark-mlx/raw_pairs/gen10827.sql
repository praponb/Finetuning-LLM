select
    case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end as last_restock_cohort,
    store_id,
    count(distinct quantity) as unique_stores,
    max(quantity) as max_quantity
from inventory
where quantity > 150
group by 1, 2
