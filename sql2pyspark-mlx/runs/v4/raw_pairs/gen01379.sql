with ranked as (select *, row_number() over (partition by store_id order by order_date desc, id) as row_num from orders where shipping_fee is not null) select * from ranked where row_num <= 3
