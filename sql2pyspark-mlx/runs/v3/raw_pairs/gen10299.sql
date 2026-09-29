select status, first_paid_at, stddev_amount from (select status, min(paid_at) first_paid_at, round(stddev(amount), 2) stddev_amount from payments group by status) sub where stddev_amount > 62
