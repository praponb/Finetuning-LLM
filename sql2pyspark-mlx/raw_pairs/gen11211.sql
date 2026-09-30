with deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        starts_on,
        ends_on,
        discount_pct
    from promotions
    qualify row_number() over (partition by channel order by starts_on desc, id) = 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(discount_pct) over (order by ends_on, id rows between unbounded preceding and current row) running_discount_pct,
        row_number() over (order by ends_on, id) seq_no
    from deduped
    where discount_pct between 0.2 and 0.29
),
scored_promotions as (
    -- Window metrics over several different partitions
    select
        id,
        starts_on,
        ends_on,
        running_discount_pct,
        lead(running_discount_pct) over w_part next_running_discount_pct,
        lag(running_discount_pct) over w_part prev_running_discount_pct,
        lag(discount_pct) over w_part prev_discount_pct,
        lead(discount_pct) over w_part next_discount_pct,
        row_number() over w_part event_seq
    from scored
    window w_part as (order by seq_no, id)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (order by running_discount_pct desc, id) running_discount_pct_bucket,
        cume_dist() over (order by event_seq desc) event_seq_cume_dist
    from scored_promotions
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        ends_on,
        running_discount_pct,
        next_running_discount_pct,
        prev_running_discount_pct,
        prev_discount_pct,
        next_discount_pct,
        event_seq,
        running_discount_pct_bucket,
        event_seq_cume_dist,
        event_seq_cume_dist - running_discount_pct_bucket event_seq_cume_dist_minus_running_discount_pct_bucket
    from enriched
    where ends_on >= date '2023-08-17'
      and prev_discount_pct between 0.12 and 0.28
),
base_promotions as (
    -- Basic cleansing and defaults
    select *, round(prev_running_discount_pct / next_running_discount_pct, 2) prev_running_discount_pct_per_next_running_discount_pct
    from base
    where ends_on > to_date('2023-07-09')
)
-- Multi-level aggregation
select
    case when next_discount_pct >= 0.29 then 'LARGE' else 'SMALL' end as next_discount_pct_tier,
    round(avg(next_running_discount_pct), 2) as avg_next_running_discount_pct,
    round(avg(prev_running_discount_pct_per_next_running_discount_pct), 2) as avg_prev_running_discount_pct_per_next_running_discount_pct,
    percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles,
    count(distinct id) as distinct_ids,
    percentile_approx(running_discount_pct_bucket, 0.95) as running_discount_pct_bucket_p95,
    count(distinct id) as distinct_ids_2
from base_promotions
group by 1
order by distinct_ids_2 desc;
