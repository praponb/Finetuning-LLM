select
    store_id,
    case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end as last_restock_cohort,
    avg(quantity) as avg_quantity,
    round(stddev(quantity), 2) as stddev_quantity
from inventory
where store_id in (2, 5)
group by store_id, case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end;
