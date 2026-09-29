select * from (select method, status, amount from payments) pivot (sum(amount) for status in ('SUCCESS', 'REFUNDED'))
