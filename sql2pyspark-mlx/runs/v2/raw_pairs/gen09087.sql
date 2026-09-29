with numbered as (select *, row_number() over (partition by product_id order by last_restock desc, id) as row_num from inventory) select * from numbered where row_num = 1;
