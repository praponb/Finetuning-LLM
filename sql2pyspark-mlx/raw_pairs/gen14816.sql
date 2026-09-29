with base as (
    select
        *,
        concat_ws('-', status, account_type) status_account_type_key,
        coalesce(status, 'DIRECT') status_clean
    from accounts
    where opened_date >= '2024-12-18'
),
enriched as (
    select
        *,
        row_number() over w_recent row_seq,
        row_number() over (partition by customer_id, account_type order by balance desc, id) event_seq,
        lead(balance, 1, 0) over (order by balance, id) next_balance,
        last_value(status, true) over w_ordered last_known_status,
        avg(balance) over (partition by status) status_avg_balance
    from base
    window w_recent as (partition by customer_id order by balance, id), w_ordered as (partition by status, customer_id order by opened_date, id rows between unbounded preceding and current row)
),
with_dims as (
    select src.*, p.segment
    from enriched src
    inner join customers p
        on src.customer_id = p.id
    where p.age >= 63
),
eligible as (
    select s.*
    from with_dims s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time < s.opened_date
        )
),
latest as (
    select *, row_number() over (partition by status, account_type order by next_balance desc nulls last, id) as row_num
    from eligible
    qualify row_num = 1
)
select
    id,
    segment,
    measure,
    value
from (
    select
        id,
        segment,
        balance,
        next_balance
    from latest
) src
unpivot include nulls (
    value for measure in (balance, next_balance)
);
