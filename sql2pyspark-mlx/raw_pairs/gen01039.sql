select * from (select *, row_number() over (partition by customer_id order by shipping_fee desc, id) as pos from orders where order_date > '2025-08-01') as r where pos = 1;
