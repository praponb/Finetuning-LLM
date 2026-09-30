with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(event_type, 1, 2) event_type_prefix,
        coalesce(page, 'N/A') page_clean
    from web_events
    where event_time >= timestamp '2023-10-26 23:00:00'
      and page is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_prefix,
        page_clean,
        avg(duration_sec) over (partition by session_id order by duration_sec, id rows between 5 preceding and 1 preceding) duration_sec_avg_5_prior,
        avg(duration_sec) over (partition by event_type, session_id order by event_time, id rows between 2 preceding and 1 preceding) duration_sec_avg_2_prior,
        sum(duration_sec) over (partition by page order by duration_sec, id rows between unbounded preceding and current row) running_duration_sec,
        dense_rank() over (partition by page order by duration_sec) duration_sec_dense_rank
    from cleaned
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_prefix,
        page_clean,
        duration_sec_avg_5_prior,
        duration_sec_avg_2_prior,
        running_duration_sec,
        duration_sec_dense_rank,
        last_value(duration_sec_avg_5_prior, true) over (order by duration_sec, id rows between unbounded preceding and current row) last_known_duration_sec_avg_5_prior,
        percent_rank() over (partition by page order by duration_sec_avg_2_prior nulls last) duration_sec_avg_2_prior_percent_rank,
        count(duration_sec_dense_rank) over (partition by event_type) event_type_count_duration_sec_dense_rank,
        sum(running_duration_sec) over (partition by page order by duration_sec, id rows between unbounded preceding and current row) cumulative_running_duration_sec,
        sum(duration_sec_avg_5_prior) over (partition by event_type order by cast(event_time as bigint) range between 604800 preceding and current row) duration_sec_avg_5_prior_sum_7d
    from enriched
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(duration_sec_avg_2_prior_percent_rank) ignore nulls over (order by duration_sec_avg_5_prior_sum_7d, id rows between unbounded preceding and current row) first_known_duration_sec_avg_2_prior_percent_rank,
        first_value(last_known_duration_sec_avg_5_prior) ignore nulls over (partition by page order by duration_sec_avg_5_prior, id rows between unbounded preceding and current row) first_known_last_known_duration_sec_avg_5_prior,
        first_value(duration_sec_dense_rank, true) over (partition by page order by duration_sec_dense_rank, id rows between unbounded preceding and current row) first_known_duration_sec_dense_rank,
        max(duration_sec_avg_2_prior) over (order by cast(event_time as bigint) range between 7776000 preceding and current row) duration_sec_avg_2_prior_max_90d,
        lead(duration_sec_avg_2_prior_percent_rank, 2, 0) over (order by duration_sec_avg_2_prior_percent_rank, id) next_duration_sec_avg_2_prior_percent_rank
    from with_windows
)
-- Aggregate with conditional metrics
select
    event_type,
    case when duration_sec_avg_2_prior_max_90d >= 461 then 'PREMIUM' else 'STANDARD' end as duration_sec_avg_2_prior_max_90d_tier,
    count(distinct id) as distinct_ids,
    min(event_time) as first_event_time,
    rank() over (partition by event_type order by count(distinct id) desc) as distinct_ids_rank
from scored
group by event_type, case when duration_sec_avg_2_prior_max_90d >= 461 then 'PREMIUM' else 'STANDARD' end
order by distinct_ids
