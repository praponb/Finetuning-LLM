select * from (select *, row_number() over (partition by store_id, product_id order by quantity desc nulls first, id) as row_num from inventory) ranked where row_num = 1;
