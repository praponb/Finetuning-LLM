select * from payments where method in ('card', 'paypal', 'cash') and to_date(paid_at) < '2024-08-23';
