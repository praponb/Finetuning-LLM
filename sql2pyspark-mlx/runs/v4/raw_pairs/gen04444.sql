with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        cast(paid_at as date) as paid_at_day
    from payments
    where to_date(paid_at) < '2025-09-01'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        paid_at_day,
        round(amount / sum(amount) over (partition by order_id), 4) as amount_share,
        last_value(amount, true) over w_hist as last_known_amount,
        avg(amount) over (partition by order_id order by paid_at asc, id asc rows between 5 preceding and current row) as amount_avg_5,
        row_number() over w_recent as row_seq,
        rank() over (partition by order_id, method order by amount asc nulls last) as amount_rank
    from cleaned
    window w_hist as (order by paid_at_day asc, id asc rows between unbounded preceding and current row), w_recent as (partition by method order by amount asc, id asc)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(amount_rank) over (partition by method, status order by paid_at_day asc, id asc rows between 5 preceding and current row) as amount_rank_avg_5,
        last_value(row_seq, true) over (partition by order_id order by amount_avg_5 asc, id asc rows between unbounded preceding and current row) as last_known_row_seq,
        percent_rank() over (order by amount_rank asc nulls last) as amount_rank_percent_rank,
        ntile(3) over w_running as last_known_amount_bucket
    from scored
    window w_running as (order by last_known_amount desc, id asc)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by method, status order by paid_at asc nulls first, id asc) as row_num
        from metrics
    ) ranked
    where row_num = 1
),
base as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(method, 'Unknown') as method_filled,
        coalesce(method, 'UNKNOWN') as method_filled_2
    from latest
    where method is not null
      and amount_rank_avg_5 between 5 and 7
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', paid_at) as quarter_start,
    date_trunc('week', paid_at) as week_start,
    case when amount_rank_avg_5 >= 7 then 'LARGE' else 'SMALL' end as amount_rank_avg_5_tier,
    sum(row_seq) as total_row_seq,
    sum(case when method = 'cash' then row_seq else 0 end) as cash_row_seq
from base
group by date_trunc('quarter', paid_at), date_trunc('week', paid_at), case when amount_rank_avg_5 >= 7 then 'LARGE' else 'SMALL' end
having cash_row_seq > 0
order by amount_rank_avg_5_tier asc nulls first
