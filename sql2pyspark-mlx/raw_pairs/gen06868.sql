with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(browser, true) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_browser,
        last_value(browser) ignore nulls over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as last_known_browser
    from sessions
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by country order by started_at asc, id asc) > interval 14 days then 1 else 0 end as new_group_flag
    from with_windows
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        browser,
        started_at,
        country,
        first_known_browser,
        last_known_browser,
        sum(new_group_flag) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by country order by started_at desc, id asc) as rank_in_group
        from islands
    ) ranked
    where rank_in_group = 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by month(started_at), country order by session_id desc) as session_id_dense_rank,
        row_number() over (partition by month(started_at), country order by session_id desc, id asc) as seq_no,
        avg(session_id) over (partition by browser order by started_at asc, id asc rows between 2 preceding and 1 preceding) as session_id_avg_2_prior,
        first_value(country, true) over (partition by browser order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_country
    from ranked
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by browser order by started_at asc, id asc) > interval '30' day then 1 else 0 end as session_start_flag
    from scored
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by browser order by started_at asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
),
remaining_keys as (
    -- Set intersection on keys
    select browser, id
    from sessionized
    intersect all
    select browser, id
    from sessions
    where customer_id in (8, 16, 22, 39)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessionized s
    join remaining_keys k on s.browser = k.browser and s.id = k.id
),
remaining_keys_2 as (
    -- Set difference on keys
    select browser
    from reconciled
    except
    select browser
    from sessions
    where started_at >= '2025-01-02'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    join remaining_keys_2 k on s.browser = k.browser
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            country,
            browser,
            session_id_avg_2_prior
        from verified
    ) src
    pivot (
        max(session_id_avg_2_prior)
        for browser in ('Firefox', 'Chrome')
    )
)
select
    country,
    Firefox,
    Chrome
from wide
order by country desc;
