select amount from payments where status in ('REFUNDED', 'FAILED') union select cost as amount from products;
