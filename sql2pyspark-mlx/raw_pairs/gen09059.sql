select *, cast(quantity as double) as quantity_num from order_items where quantity >= 5 and product_id is not null order by order_id desc, 3 desc
