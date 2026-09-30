with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        trim(account_type) as clean_account_type
    from accounts
    where account_type = 'CREDIT'
),
scored as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over (partition by status order by balance asc, id asc) as balance_bucket,
        lag(balance, 1) over (partition by account_type order by balance asc, id asc) as prev_balance,
        last_value(status) ignore nulls over (partition by status order by opened_date asc, id asc rows between unbounded preceding and current row) as last_known_status
    from cleaned
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by status order by prev_balance desc, id asc) as event_seq,
        max(balance) over (partition by account_type) as account_type_max_balance,
        avg(balance_bucket) over (order by balance asc, id asc rows between 5 preceding and current row) as balance_bucket_avg_5,
        last_value(balance_bucket) ignore nulls over (partition by account_type, status order by balance_bucket asc, id asc rows between unbounded preceding and current row) as last_known_balance_bucket
    from scored
),
metrics_accounts as (
    -- Ranking and gap features
    select
        *,
        sum(account_type_max_balance) over w_running as cumulative_account_type_max_balance,
        lag(balance, 1) over w_part as prev_balance_2,
        ntile(5) over w_part_2 as account_type_max_balance_bucket
    from metrics
    window w_running as (order by account_type_max_balance asc, id asc rows between unbounded preceding and current row), w_part as (partition by account_type order by account_type_max_balance asc, id asc), w_part_2 as (partition by status, account_type order by account_type_max_balance asc, id asc)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics_accounts s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_type = 'FEE'
        )
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.transaction_merchant,
        x.transaction_amount
    from kept s
    left join lateral (
            select c.merchant as transaction_merchant, c.amount as transaction_amount
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
)
-- Ranking and gap features
select
    *,
    lag(prev_balance, 2, 0) over (partition by account_type order by balance_bucket_avg_5 asc, id asc) as prev_prev_balance,
    percent_rank() over (partition by status, account_type order by last_known_balance_bucket asc nulls first) as last_known_balance_bucket_percent_rank,
    cume_dist() over (partition by status order by account_type_max_balance_bucket desc nulls first) as account_type_max_balance_bucket_cume_dist,
    lead(transaction_amount, 2) over (partition by status, transaction_merchant order by last_known_balance_bucket asc, id asc) as next_transaction_amount
from with_last
