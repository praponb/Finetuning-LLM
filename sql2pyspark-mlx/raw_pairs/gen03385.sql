with cleaned as (
    select
        *,
        concat(account_type, ', ', status) as account_type_status,
        coalesce(status, 'OTHER') as status_filled
    from accounts
    where opened_date between '2023-06-26' and '2024-04-10'
),
not_flagged as (
    select s.*
    from cleaned s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and hour(x.txn_time) between 8 and 12
        )
),
metrics as (
    select
        *,
        lag(balance) over w_grp as prev_balance,
        row_number() over w_grp as event_seq,
        dense_rank() over (partition by account_type, customer_id order by balance nulls last) as balance_dense_rank
    from not_flagged
    where balance >= 68000
    window w_grp as (partition by status order by opened_date, id)
),
base as (
    select
        id,
        customer_id,
        account_type,
        balance,
        status,
        account_type_status,
        status_filled,
        prev_balance,
        event_seq,
        balance_dense_rank,
        date_add(opened_date, 74) as opened_date_plus_74d,
        coalesce(status, 'OTHER') as status_filled_2
    from metrics
    where status is not distinct from 'OPEN'
),
metrics_accounts as (
    select
        *,
        sum(balance_dense_rank) over (partition by customer_id order by event_seq, id rows between 2 preceding and current row) as last3_sum_balance_dense_rank,
        avg(balance_dense_rank) over (partition by customer_id order by event_seq, id rows between 2 preceding and current row) as last3_avg_balance_dense_rank,
        max(balance_dense_rank) over (partition by customer_id order by event_seq, id rows between 2 preceding and current row) as last3_max_balance_dense_rank
    from base
)
select
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.status,
    s.account_type_status,
    s.prev_balance,
    s.opened_date_plus_74d,
    s.last3_sum_balance_dense_rank,
    (select min(x.discount) from orders x where x.customer_id = s.customer_id) as min_order_discount
from metrics_accounts s
order by balance nulls first
