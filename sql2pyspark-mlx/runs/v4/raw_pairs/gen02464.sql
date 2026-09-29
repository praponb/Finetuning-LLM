select amount, status, case when method = 'paypal' then 1 else 0 end is_paypal from payments where method in ('card', 'bank_transfer', 'promptpay') order by status, is_paypal desc;
