select quantity, last_restock, product_id
from inventory
where last_restock between '2024-10-01' and '2024-10-25' and product_id in (2, 4, 5, 6)
order by 1 desc
