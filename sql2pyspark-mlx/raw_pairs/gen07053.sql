select method, avg(amount) as avg_amount from payments where method != 'paypal' group by 1;
