select * from (select method, status, amount from payments where order_id is not null) pivot (count(amount) for status in ('SUCCESS', 'FAILED'))
