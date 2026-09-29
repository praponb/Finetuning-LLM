with latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        s.country,
        x.web_event_event_type
    from sessions s
    join lateral (
            select c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.duration_sec != 294
        )
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        first_value(country) ignore nulls over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        row_number() over (order by started_at, id) as event_seq
    from kept
),
latest_match_sessions as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.web_event_page,
        x.web_event_event_type_2
    from with_windows s
    join lateral (
            select c.page as web_event_page, c.event_type as web_event_event_type_2
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    -- Ranking and gap features
    select
        *,
        count(event_seq) over (partition by device, web_event_page order by cast(started_at as bigint) range between 7776000 preceding and current row) as event_seq_count_90d,
        sum(event_seq) over (partition by customer_id, browser order by cast(started_at as bigint) range between 604800 preceding and current row) as event_seq_sum_7d,
        row_number() over (partition by customer_id order by started_at, id) as row_seq,
        last_value(event_seq) ignore nulls over (partition by country order by started_at, id rows between unbounded preceding and current row) as last_known_event_seq,
        last_value(browser) ignore nulls over w_grp as last_known_browser
    from latest_match_sessions
    window w_grp as (partition by web_event_event_type_2 order by started_at, id rows between unbounded preceding and current row)
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.duration_sec <= 266
        )
)
-- Window metrics over several different partitions
select
    id,
    customer_id,
    device,
    browser,
    web_event_event_type,
    first_known_country,
    event_seq_count_90d,
    event_seq_sum_7d,
    row_seq,
    last_known_event_seq,
    last_known_browser,
    ntile(10) over (partition by year(started_at), web_event_event_type order by last_known_event_seq desc, id) as last_known_event_seq_bucket,
    sum(event_seq_count_90d) over (partition by web_event_page order by started_at, id rows between unbounded preceding and current row) as running_event_seq_count_90d
from not_flagged
where row_seq between 3 and 8
