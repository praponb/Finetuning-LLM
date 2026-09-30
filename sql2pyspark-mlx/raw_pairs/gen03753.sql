select *
from (
    select *, row_number() over (partition by store_id order by last_restock desc, id) as rn
    from inventory
    where last_restock < '2023-06-01'
) as t
where rn = 1;
