select
    status,
    account_type,
    stddev_balance,
    balance_count
from (
    select
        status,
        account_type,
        round(stddev(balance), 2) as stddev_balance,
        count(balance) as balance_count
    from accounts
    where account_type = 'CHECKING'
    group by status, account_type
) as agg
where stddev_balance > 34;
