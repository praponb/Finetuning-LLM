select *
from (
    select
        status,
        account_type,
        max(opened_date) as latest_opened_date,
        round(stddev(balance), 2) as stddev_balance
    from accounts
    where opened_date >= '2025-05-01'
    group by status, account_type
) as x
where stddev_balance >= 46
order by account_type, latest_opened_date desc;
