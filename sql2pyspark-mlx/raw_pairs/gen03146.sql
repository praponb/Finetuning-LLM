select *, row_number() over (partition by product_id order by last_restock desc nulls last, id) as rank_in_group from inventory qualify rank_in_group = 1;
