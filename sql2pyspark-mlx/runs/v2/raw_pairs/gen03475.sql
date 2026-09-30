select * from (select status, method, amount from payments) pivot (max(amount) for method in ('promptpay', 'card', 'paypal', 'bank_transfer'))
