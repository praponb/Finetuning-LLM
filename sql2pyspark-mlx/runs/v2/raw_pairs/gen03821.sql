select status, 'order' source from orders union all select status, 'payment' source from payments;
