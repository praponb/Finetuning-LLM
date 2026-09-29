with numbered as (
    select *, rank() over (partition by product_id order by last_restock desc) as rnk
    from inventory
)
select id, product_id, last_restock
from numbered
where rnk <= 3;
