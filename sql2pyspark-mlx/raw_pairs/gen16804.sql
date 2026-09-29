select * from (select channel, status, amount from orders) src pivot (max(amount) as total, count(1) as n for status in ('PENDING', 'RETURNED', 'CANCELLED'));
