select
    date_format(paid_at, 'yyyy-MM') as paid_month,
    year(paid_at) as paid_year,
    round(stddev(amount), 2) as stddev_amount,
    sum(amount) as total_amount
from payments
group by date_format(paid_at, 'yyyy-MM'), year(paid_at);
