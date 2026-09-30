select * from (select *, row_number() over (partition by status order by shipping_fee, id) as row_num from orders) as ranked where row_num = 1 order by discount desc
