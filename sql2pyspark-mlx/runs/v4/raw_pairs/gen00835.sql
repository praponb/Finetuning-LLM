with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(status) as clean_status,
        round(amount * 0.85, 2) as adjusted_amount,
        coalesce(method, 'OTHER') as method_clean
    from payments
    where paid_at >= timestamp '2024-03-25 04:00:00'
      and method is not null
      and amount <= 410
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        clean_status,
        adjusted_amount,
        method_clean,
        sum(amount) over (partition by status order by adjusted_amount, id rows between unbounded preceding and current row) as cumulative_amount,
        lag(adjusted_amount, 2) over w_recent as prev_adjusted_amount
    from valid
    window w_recent as (partition by order_id order by amount, id)
),
metrics_payments as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by status order by adjusted_amount desc) as adjusted_amount_percent_rank,
        row_number() over (partition by status order by paid_at desc, id) as event_seq,
        lag(amount, 1, 0) over (partition by order_id order by prev_adjusted_amount, id) as prev_amount
    from metrics
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        clean_status,
        adjusted_amount,
        method_clean,
        prev_adjusted_amount,
        adjusted_amount_percent_rank,
        event_seq,
        ceil(adjusted_amount_percent_rank) as adjusted_amount_percent_rank_whole,
        case when method = 'card' then 1 else 0 end as is_card
    from metrics_payments
    where event_seq <= 4
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        order_id,
        adjusted_amount,
        prev_adjusted_amount,
        adjusted_amount_percent_rank,
        amount
    from base
) src
unpivot (
    val for kpi in (adjusted_amount, prev_adjusted_amount, adjusted_amount_percent_rank, amount)
);
