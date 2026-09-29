select status from orders where customer_id is null intersect select status from shipments
