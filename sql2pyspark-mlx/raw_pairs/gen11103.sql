with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        ends_on,
        min(discount_pct) over w_grp as running_min_discount_pct,
        max(discount_pct) over w_grp as running_max_discount_pct,
        count(discount_pct) over w_grp as running_count_discount_pct
    from promotions
    window w_grp as (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row)
),
filtered as (
    -- Basic cleansing and defaults
    select *, cast(running_max_discount_pct as int) as running_max_discount_pct_int
    from metrics
    where ends_on > date '2023-11-20'
      and month(ends_on) in (6, 8, 12)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over w_hist as running_max_discount_pct_percent_rank,
        lead(running_max_discount_pct_int, 2) over (partition by year(ends_on) order by ends_on, id) as next_running_max_discount_pct_int,
        sum(running_max_discount_pct_int) over (partition by year(ends_on) order by running_max_discount_pct_int, id rows between unbounded preceding and current row) as cumulative_running_max_discount_pct_int
    from filtered
    where running_max_discount_pct_int = 26
    window w_hist as (order by running_max_discount_pct)
),
filtered_promotions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        ends_on,
        running_min_discount_pct,
        running_max_discount_pct,
        running_max_discount_pct_percent_rank,
        next_running_max_discount_pct_int,
        cumulative_running_max_discount_pct_int,
        case when cumulative_running_max_discount_pct_int >= 437 then 'High' when cumulative_running_max_discount_pct_int >= 181 then 'Medium' else 'Low' end as cumulative_running_max_discount_pct_int_band
    from scored
    where ends_on > date '2023-10-04'
      and running_max_discount_pct >= 0.24
),
kept_keys as (
    -- Set intersection on keys
    select id
    from filtered_promotions
    intersect all
    select id
    from promotions
    where channel = 'partner'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered_promotions s
    inner join kept_keys k on s.id = k.id
)
select
    id,
    ends_on,
    running_min_discount_pct,
    running_max_discount_pct,
    running_max_discount_pct_percent_rank,
    cumulative_running_max_discount_pct_int
from verified
