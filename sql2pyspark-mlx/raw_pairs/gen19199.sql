select *, cast(quantity as double) as quantity_num from order_items where order_id in (7, 21, 33, 36)
