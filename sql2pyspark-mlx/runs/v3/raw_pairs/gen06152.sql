select * from (select channel, status, discount from orders where discount between 0.21 and 0.4) pivot (count(discount) for status in ('COMPLETED', 'PENDING')) order by channel;
