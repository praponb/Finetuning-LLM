select quarter(paid_at) as paid_quarter, sum(amount) as total_amount, round(stddev(amount), 2) as stddev_amount from payments group by 1;
