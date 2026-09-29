select *
from (
    select month(txn_time) as txn_month, round(stddev(amount), 2) as stddev_amount
    from transactions
    where amount is not null
    group by month(txn_time)
) as agg
where stddev_amount >= 48
order by txn_month asc
limit 1
