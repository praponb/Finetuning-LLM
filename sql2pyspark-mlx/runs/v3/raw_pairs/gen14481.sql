select *
from (
    select case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end last_restock_cohort, count(*) total_count
    from inventory
    group by case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end
) sub
where total_count >= 5;
