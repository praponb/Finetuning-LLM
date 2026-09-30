select * from (select *, row_number() over (partition by category order by stock_qty desc, id) as rank_in_group from products) t where rank_in_group = 1;
