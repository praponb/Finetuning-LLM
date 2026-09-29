select * from (select id, status, shipping_fee, rank() over (partition by status order by shipping_fee desc) as row_num from orders) as t where row_num = 1;
