with base as (
    select
        *,
        concat_ws(',', account_type, status) as account_type_status_key,
        coalesce(status, 'NONE') as status_clean
    from accounts
    where balance > 56000
),
not_flagged as (
    select s.*
    from base s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.amount > 1600
        )
),
cleaned as (
    select
        id,
        customer_id,
        account_type,
        balance,
        status,
        account_type_status_key,
        status_clean,
        date_add(opened_date, 82) as opened_date_plus_82d,
        coalesce(status, 'N/A') as resolved_status
    from not_flagged
    where customer_id is not null
),
not_flagged_accounts as (
    select s.*
    from cleaned s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.is_flagged
        )
),
grouped as (
    select
        case when balance >= 88000 then 'LARGE' else 'SMALL' end as balance_tier,
        approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
        approx_percentile(balance, 0.9) as balance_p90,
        sum(balance) as total_balance,
        sort_array(collect_set(account_type)) as account_type_list,
        count_if(account_type = 'SAVINGS') as savings_events
    from not_flagged_accounts
    group by balance_tier
    having total_balance < 0
)
select
    balance_tier,
    balance_quartiles,
    balance_p90,
    total_balance,
    account_type_list,
    savings_events
from grouped
order by balance_p90 desc nulls first
