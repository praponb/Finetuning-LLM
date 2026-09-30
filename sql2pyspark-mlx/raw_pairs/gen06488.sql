select * from (select status, method, amount from payments)
pivot (avg(amount) for method in ('bank_transfer', 'promptpay', 'card', 'cash'));
