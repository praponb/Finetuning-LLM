with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        status,
        case when account_type = 'SAVINGS' then 1 else 0 end as is_savings,
        cast(balance as int) as balance_int,
        coalesce(status, 'DIRECT') as resolved_status
    from accounts
    where status in ('FROZEN', 'CLOSED')
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(is_savings) over (partition by status order by balance_int, id rows between 5 preceding and 1 preceding) as is_savings_avg_5_prior,
        avg(balance_int) over (partition by status order by balance_int, id rows between 3 preceding and 1 preceding) as balance_int_avg_3_prior,
        avg(is_savings) over (partition by status order by is_savings, id rows between 3 preceding and 1 preceding) as is_savings_avg_3_prior,
        row_number() over (partition by status order by balance_int desc, id) as event_seq,
        sum(balance_int) over (partition by status order by balance_int, id rows between unbounded preceding and current row) as running_balance_int
    from cleaned
    where is_savings < 80
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by status order by balance_int desc) as balance_int_percent_rank,
        ntile(4) over (partition by status order by event_seq, id) as event_seq_bucket,
        sum(event_seq) over (partition by status order by is_savings_avg_3_prior, id rows between unbounded preceding and current row) as cumulative_event_seq
    from metrics
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(is_savings) over (partition by status) as status_min_is_savings,
        first_value(running_balance_int) ignore nulls over (partition by status order by balance_int_percent_rank, id rows between unbounded preceding and current row) as first_known_running_balance_int
    from enriched
)
select
    id,
    status,
    is_savings,
    balance_int,
    balance_int_avg_3_prior,
    is_savings_avg_3_prior,
    running_balance_int,
    balance_int_percent_rank,
    event_seq_bucket,
    case when status rlike '^(FROZEN|OPEN)$' then 'EXTENDED' else 'CORE' end as status_zone
from scored
order by status_zone desc
