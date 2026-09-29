with ranked as (select *, row_number() over (partition by product_id order by last_restock desc, id) as rn from inventory) select * from ranked where rn <= 3 order by product_id desc, id desc;
