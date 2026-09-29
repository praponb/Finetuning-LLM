select status from accounts intersect select status from payments where order_id is not null order by status desc;
