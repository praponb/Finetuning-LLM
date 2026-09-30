select case when year(stores.opened_date) < 2024 then 'before_2024' when year(stores.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, sum(inventory.quantity) as total_quantity
from inventory
join stores on inventory.store_id = stores.id
group by case when year(stores.opened_date) < 2024 then 'before_2024' when year(stores.opened_date) = 2024 then '2024' else 'after_2024' end
order by total_quantity desc
