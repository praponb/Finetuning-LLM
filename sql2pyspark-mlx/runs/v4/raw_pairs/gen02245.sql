with valid as (
    select
        *,
        trim(account_type) as clean_account_type,
        lower(account_type) as account_type_lower
    from accounts
    where customer_id is not null
      and status = 'OPEN'
),
cleaned as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        clean_account_type,
        coalesce(balance, 0) as balance_filled,
        trim(account_type) as clean_account_type_2,
        coalesce(status, 'N/A') as resolved_status
    from valid
    where opened_date between '2024-12-12' and '2025-07-01'
),
metrics as (
    select
        *,
        ntile(4) over (partition by account_type, status order by balance, id) as balance_bucket,
        ntile(10) over w_hist as balance_bucket_2
    from cleaned
    window w_hist as (order by balance desc, id)
)
select *
from (
    select
        account_type,
        customer_id,
        status,
        balance
    from metrics
) src
pivot (
    sum(balance)
    for status in ('CLOSED', 'OPEN')
);
