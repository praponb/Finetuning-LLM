with cleaned as (
    select
        *,
        coalesce(status, 'N/A') status_filled,
        round(balance * 1.1, 2) adjusted_balance,
        coalesce(status, 'UNKNOWN') status_clean
    from accounts
    where opened_date > date '2023-07-26'
      and customer_id in (34, 40)
),
ranked as (
    select
        id,
        customer_id,
        account_type,
        balance,
        status_filled,
        status_clean
    from (
        select *, rank() over (partition by account_type, customer_id order by opened_date desc) as rank_in_group
        from cleaned
    ) t
    where rank_in_group = 1
),
valid as (
    select
        *,
        case when account_type = 'CHECKING' then 1 else 0 end is_checking,
        lower(account_type) account_type_lower
    from ranked
    where customer_id is not null
),
kept as (
    select s.*
    from valid s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time >= '2025-05-03'
        )
)
select *
from (
    select
        customer_id,
        account_type,
        balance
    from kept
) src
pivot (
    avg(balance) as value_sum
    for account_type in ('SAVINGS' as savings, 'LOAN' as loan)
);
