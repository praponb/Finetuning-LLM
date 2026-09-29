select * from (select status, method, amount from payments)
pivot (sum(amount) for method in ('card', 'bank_transfer', 'paypal'))
