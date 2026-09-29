select * from (select hour(paid_at) as paid_hour, method, amount from payments where method in ('bank_transfer', 'cash', 'card'))
pivot (avg(amount) for method in ('bank_transfer', 'card', 'paypal', 'cash'));
