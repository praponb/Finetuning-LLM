select status from orders where order_date > '2023-05-01' union all select status from shipments;
