select * from (select method, status, amount from payments) pivot (count(amount) for status in ('REFUNDED', 'FAILED', 'SUCCESS')) order by method;
