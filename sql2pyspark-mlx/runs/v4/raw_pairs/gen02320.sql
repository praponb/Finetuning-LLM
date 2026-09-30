with deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by method, status order by paid_at desc nulls first) as rn
        from payments
    ) x
    where rn = 1
),
deduped_payments as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by order_id, status order by amount desc nulls last, id) as rn_2
        from deduped
    ) x
    where rn_2 = 1
),
joined as (
    -- Attach orders attributes
    select s.*, p.channel
    from deduped_payments s
    left join orders p
        on s.order_id = p.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by order_id order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at, id) + interval '90' day then 1 else 0 end as session_start_flag
    from joined
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
deduped_payments_2 as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by status order by rn, id) as latest_rank
        from islands
    ) ranked
    where latest_rank = 1
),
grouped as (
    -- Grouped metrics
    select
        case when session_id >= 8 then 'PREMIUM' else 'STANDARD' end as session_id_tier,
        size(collect_set(status)) as num_status,
        min(session_start_flag) as min_session_start_flag,
        count(*) as cnt,
        count(*) as cnt_2
    from deduped_payments_2
    group by case when session_id >= 8 then 'PREMIUM' else 'STANDARD' end
)
select session_id_tier, num_status
from grouped
order by session_id_tier desc
