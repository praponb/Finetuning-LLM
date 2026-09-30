select status, 'order' as source from orders union select status, 'payment' as source from payments;
