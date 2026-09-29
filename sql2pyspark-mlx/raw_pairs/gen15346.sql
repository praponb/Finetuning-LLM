with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        case when amount >= 3100 then 'High' when amount >= 1700 then 'Medium' else 'Low' end as amount_level,
        length(txn_type) as txn_type_length,
        coalesce(merchant, 'UNKNOWN') as merchant_filled
    from transactions
    where merchant is not null
),
joined as (
    -- Attach accounts attributes
    select
        src.*,
        b.account_type,
        b.status
    from filtered src
    inner join accounts b
        on src.account_id = b.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_type,
        account_type,
        status,
        avg(txn_type_length) over w_recent as txn_type_length_avg_5_prior,
        first_value(account_type) ignore nulls over (partition by account_type, account_id order by txn_type_length, id rows between unbounded preceding and current row) as first_known_account_type
    from joined
    window w_recent as (partition by status, account_id order by txn_time, id rows between 5 preceding and 1 preceding)
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from scored
    intersect all
    select id
    from transactions
    where txn_type != 'REFUND'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.id = k.id
),
enriched as (
    -- Attach accounts attributes
    select
        e.account_id,
        e.status,
        e.txn_type_length_avg_5_prior,
        p.status as account_status
    from kept_rows e
    inner join accounts p
        on e.account_id = p.id
),
enriched_transactions as (
    -- Attach accounts attributes
    select
        e.account_id,
        e.txn_type_length_avg_5_prior,
        b.status as account_status_2,
        b.balance,
        b.opened_date
    from enriched e
    left join accounts b
        on e.account_id = b.id
    where b.balance >= 29000
),
joined_transactions as (
    -- Attach accounts attributes
    select
        e.account_id,
        e.account_status_2,
        p.status
    from enriched_transactions e
    inner join accounts p
        on e.account_id = p.id
),
summary as (
    -- Aggregate with conditional metrics
    select
        status,
        count(case when account_status_2 = 'OPEN' then 1 end) as open_count,
        sort_array(collect_set(account_status_2)) as account_status_2_list,
        count_if(account_status_2 = 'FROZEN') as frozen_events
    from joined_transactions
    group by rollup (status)
)
select
    status,
    open_count,
    account_status_2_list,
    frozen_events
from summary
order by status desc nulls last;
