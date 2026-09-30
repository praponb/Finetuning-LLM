select id, store_id, quantity, product_id from (select *, rank() over (partition by store_id order by quantity desc) as rnk from inventory) as t where rnk = 1 order by store_id;
