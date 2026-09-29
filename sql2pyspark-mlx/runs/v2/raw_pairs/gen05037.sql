select status, round(stddev(amount), 2) stddev_amount
from payments
where status = 'REFUNDED'
group by status;
