with ranked_inventory as (select *, row_number() over (partition by product_id order by quantity desc, id) as rnk from inventory) select * from ranked_inventory where rnk <= 3;
