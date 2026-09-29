select order_id, status from payments where order_id = 18 intersect select order_id, status from payments where amount between 690 and 740
