select id, last_restock, cast(quantity as string) as quantity_str from inventory order by id desc;
