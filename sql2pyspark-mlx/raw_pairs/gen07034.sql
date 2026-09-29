select store_id, product_id, count(*) as cnt
from inventory
where last_restock between '2023-08-01' and '2025-08-01' or quantity is not null
group by store_id, product_id
having cnt < 3
order by 3 desc, store_id desc;
