select method, round(stddev(amount), 2) as stddev_amount from payments group by method order by 1 desc limit 50;
