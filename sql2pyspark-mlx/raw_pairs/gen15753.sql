with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        method,
        amount,
        status,
        count(amount) over w_grp as last4_count_amount,
        avg(amount) over w_grp as last4_avg_amount,
        sum(amount) over w_grp as last4_sum_amount,
        min(amount) over w_grp as last4_min_amount,
        sum(amount) over w_hist as cumulative_amount
    from payments
    window w_grp as (partition by status order by paid_at, id rows between 3 preceding and current row), w_hist as (partition by order_id order by paid_at, id rows between unbounded preceding and current row)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by status order by last4_count_amount, id) as seq_no,
        row_number() over (partition by method order by cumulative_amount desc, id) as row_seq
    from with_windows
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by status order by last4_min_amount desc, id) as row_num
        from scored
    ) x
    where row_num = 1
),
scored_payments as (
    -- Running and moving aggregates
    select
        *,
        max(last4_min_amount) over w_running as running_max_last4_min_amount,
        sum(last4_min_amount) over w_running as running_sum_last4_min_amount,
        min(last4_min_amount) over w_running as running_min_last4_min_amount
    from ranked
    window w_running as (partition by method order by cumulative_amount, id rows between unbounded preceding and current row)
),
ranked_payments as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status, method order by last4_count_amount desc, id) as latest_rank
    from scored_payments
    qualify latest_rank = 1
),
kept_keys as (
    -- Set difference on keys
    select id, method
    from ranked_payments
    except
    select id, method
    from payments
    where method = 'card'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked_payments s
    inner join kept_keys k on s.id = k.id and s.method = k.method
)
select
    method,
    amount,
    status,
    last4_sum_amount,
    last4_min_amount,
    cumulative_amount,
    seq_no,
    latest_rank,
    case when status rlike '^(FAILED|SUCCESS)$' then 'PRIORITY' else 'CORE' end as status_zone
from reconciled
order by latest_rank nulls first
