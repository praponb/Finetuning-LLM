select channel, max_amount from (select channel, max(amount) as max_amount from orders group by channel) as t where max_amount > 840 order by max_amount desc, channel limit 1;
