select channel, round(stddev(amount), 2) as stddev_amount from orders group by channel having round(stddev(amount), 2) > 58 order by stddev_amount desc, channel limit 10;
