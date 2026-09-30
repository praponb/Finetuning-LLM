select
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock
from inventory i
where i.store_id in (select id from stores where opened_date < '2023-09-11');
