with filtered as (
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        day(delivered_date) as delivered_date_day
    from shipments
    where delivered_date >= date '2023-03-13'
      and carrier is not null
),
cleaned as (
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        case when status in ('RETURNED', 'IN_TRANSIT') then 'Tier 1' else 'Other' end as status_group,
        add_months(delivered_date, 1) as delivered_date_plus_months
    from filtered
    where status <> 'LOST'
),
with_windows as (
    select
        id,
        delivered_date,
        cost,
        status,
        status_group,
        delivered_date_plus_months,
        round(cost / sum(cost) over (partition by status), 4) as cost_share,
        row_number() over w_running as seq_no,
        avg(cost) over w_hist as cost_avg_6_prior,
        row_number() over (order by delivered_date desc, id) as row_seq,
        round(cost / sum(cost) over (partition by status), 4) as cost_share_2
    from cleaned
    where status = 'LOST'
    window w_running as (partition by carrier, status order by delivered_date_plus_months, id), w_hist as (order by cost, id rows between 6 preceding and 1 preceding)
),
enriched as (
    select
        id,
        cost,
        status,
        status_group,
        seq_no,
        row_seq,
        cost_share_2,
        lag(seq_no) over w_recent as prev_seq_no,
        lead(seq_no) over w_recent as next_seq_no,
        lag(row_seq) over w_recent as prev_row_seq,
        lead(row_seq) over w_recent as next_row_seq,
        row_number() over w_recent as seq_no_2,
        percent_rank() over (partition by status order by cost_share desc) as cost_share_percent_rank
    from with_windows
    window w_recent as (partition by status order by delivered_date, id)
)
select
    id,
    status_group,
    row_seq,
    cost_share_2,
    prev_seq_no,
    next_seq_no,
    prev_row_seq,
    next_row_seq,
    cost_share_percent_rank
from enriched
