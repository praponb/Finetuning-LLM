with metrics as (
    select
        id,
        order_id,
        method,
        paid_at,
        lag(amount) over w_recent as prev_amount,
        row_number() over w_recent as row_seq
    from payments
    window w_recent as (partition by order_id order by paid_at, id)
)
select
    method,
    case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
    date_trunc('year', paid_at) as year_start,
    round(avg(row_seq), 2) as avg_row_seq,
    max(paid_at) as ended_at,
    size(collect_set(method)) as num_methods,
    grouping(method) as grouping_id
from metrics
group by grouping sets ((method, case when row_seq >= 7 then 'LARGE' else 'SMALL' end, date_trunc('year', paid_at)), (method, case when row_seq >= 7 then 'LARGE' else 'SMALL' end), ())
order by row_seq_tier
