with filtered as (
    select
        id,
        method,
        paid_at,
        status,
        round(amount * 100, 2) as amount_scaled,
        length(status) as status_length
    from payments
    where order_id is not null
),
with_windows as (
    select
        *,
        avg(status_length) over (partition by status, method order by paid_at, id rows between 5 preceding and current row) as status_length_avg_5,
        row_number() over (partition by method order by status_length desc, id) as seq_no
    from filtered
),
latest as (
    select *, row_number() over (partition by status, method order by paid_at desc nulls last, id) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
),
enriched as (
    select
        id,
        method,
        paid_at,
        status_length,
        status_length_avg_5,
        seq_no,
        sum(status_length) over (partition by status order by cast(paid_at as bigint) range between 2592000 preceding and current row) as status_length_sum_30d,
        max(amount_scaled) over (partition by status) as status_max_amount_scaled,
        sum(amount_scaled) over (partition by method order by dedup_rank, id rows between unbounded preceding and current row) as cumulative_amount_scaled
    from latest
    where status_length = 23
)
select
    id,
    method,
    paid_at,
    status_length_avg_5,
    seq_no,
    status_length_sum_30d,
    cumulative_amount_scaled,
    case when method rlike '^(bank_transfer|cash)$' then 'GROUP_B' when method rlike '^(card|paypal)$' then 'EXTENDED' else 'PRIORITY' end as method_zone
from enriched
order by status_length_avg_5 desc, seq_no;
