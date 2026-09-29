with filtered as (
    select
        id,
        channel,
        starts_on,
        ends_on,
        case when discount_pct >= 0.33 then 'High' when discount_pct >= 0.16 then 'Medium' else 'Low' end discount_pct_tier,
        coalesce(channel, 'NONE') channel_clean
    from promotions
    where channel is not null
      and promo_code in ('BUNDLE', 'VIP', 'WELCOME5')
),
scored as (
    select
        *,
        row_number() over (partition by channel order by starts_on desc, id) seq_no,
        row_number() over (partition by channel order by starts_on, id) seq_no_2
    from filtered
),
with_windows as (
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct_tier,
        channel_clean,
        seq_no,
        seq_no_2,
        max(seq_no_2) over (partition by channel) channel_max_seq_no_2,
        avg(seq_no_2) over (partition by channel order by ends_on, id rows between 2 preceding and 1 preceding) seq_no_2_avg_2_prior,
        lag(seq_no, 1) over (partition by channel order by ends_on, id) prev_seq_no,
        rank() over (order by seq_no nulls last) seq_no_rank
    from scored
),
metrics as (
    select
        *,
        ntile(4) over (partition by channel order by seq_no desc, id) seq_no_bucket,
        row_number() over (order by prev_seq_no, id) row_seq,
        max(seq_no) over w_hist channel_max_seq_no,
        round(prev_seq_no / sum(prev_seq_no) over (partition by channel), 4) prev_seq_no_share
    from with_windows
    window w_hist as (partition by channel)
)
select
    channel,
    case when seq_no_2 >= 9 then 'HIGH' else 'LOW' end as seq_no_2_tier,
    size(collect_set(channel)) as num_channels,
    sort_array(collect_set(channel)) as channel_list,
    grouping(channel) as gid
from metrics
group by grouping sets ((channel, case when seq_no_2 >= 9 then 'HIGH' else 'LOW' end), (channel), ())
having num_channels < 1
order by channel desc nulls first;
