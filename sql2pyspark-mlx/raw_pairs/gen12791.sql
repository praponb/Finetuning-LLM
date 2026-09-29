with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(amount * 0.85, 2) adjusted_amount,
        case when amount >= 4000 then 'High' when amount >= 1300 then 'Medium' else 'Low' end amount_band,
        coalesce(merchant, 'N/A') merchant_filled
    from transactions
    where merchant is not null
      and merchant is not distinct from 'Grab'
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by txn_type order by amount desc nulls first) as rank_in_group
        from cleaned
    ) x
    where rank_in_group = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by account_id order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time, id) + interval '30' day then 1 else 0 end as is_new_session
    from deduped
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
joined as (
    -- Attach accounts attributes
    select
        a.*,
        p.opened_date,
        p.account_type
    from sessionized a
    left join accounts p
        on a.account_id = p.id
),
deduped_transactions as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by merchant, txn_type order by rank_in_group desc) as latest_rank
    from joined
    qualify latest_rank = 1
),
with_dims as (
    -- Attach accounts attributes
    select
        src.id,
        src.account_id,
        src.txn_time,
        src.is_flagged,
        src.adjusted_amount,
        src.merchant_filled,
        src.rank_in_group,
        src.account_type,
        src.latest_rank,
        d.opened_date as account_opened_date,
        d.status,
        d.account_type as account_account_type
    from deduped_transactions src
    left join accounts d
        on src.account_id = d.id
    where d.opened_date >= '2024-10-01'
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            account_type,
            status,
            adjusted_amount
        from with_dims
    ) src
    pivot (
        avg(adjusted_amount) as total,
        count(1) as n
        for status in ('FROZEN', 'OPEN')
    )
)
select
    account_type,
    FROZEN_total,
    OPEN_total,
    case when account_type rlike '^(CHECKING|LOAN)$' then 'GROUP_B' else 'PRIORITY' end as account_type_zone
from by_type
order by FROZEN_total nulls first;
