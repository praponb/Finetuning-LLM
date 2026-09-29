select round(stddev(amount), 2) as stddev_amount, count(amount) as amount_count
from transactions
where account_id in (16, 24, 32)
order by amount_count desc;
