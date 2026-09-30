select status, stddev_balance
from (
    select status, round(stddev(balance), 2) as stddev_balance
    from accounts
    group by status
) as agg
where stddev_balance < 45
order by stddev_balance desc;
