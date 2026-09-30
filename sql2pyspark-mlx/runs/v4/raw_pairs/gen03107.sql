select * from (select status, method, amount from payments)
pivot (count(amount) for method in ('bank_transfer', 'card', 'promptpay', 'paypal'));
