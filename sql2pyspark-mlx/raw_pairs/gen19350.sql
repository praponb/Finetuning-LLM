with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        device,
        started_at,
        first_value(browser) ignore nulls over (partition by country order by started_at, id rows between unbounded preceding and current row) as first_known_browser
    from sessions
),
with_windows_sessions as (
    -- Window metrics over several different partitions
    select
        id,
        device,
        started_at,
        row_number() over (partition by device order by started_at desc, id) as event_seq,
        row_number() over (partition by customer_id order by started_at desc, id) as row_seq
    from with_windows
    where customer_id = 14
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows_sessions s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.page in ('/checkout', '/home')
        )
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.device,
        s.started_at,
        s.event_seq,
        s.row_seq,
        x.page,
        x.web_event_event_type
    from kept s
    join lateral (
            select c.page, c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) x on true
)
-- Aggregate with conditional metrics
select
    page,
    case when row_seq >= 9 then 'LARGE' else 'SMALL' end as row_seq_tier,
    max(event_seq) as max_event_seq,
    round(sum(event_seq) / nullif(count(case when page = '/product' then 1 end), 0), 2) as event_seq_per_product,
    count(*) filter (where web_event_event_type = 'search') as search_rows,
    percentile_approx(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    count(*) as num_rows,
    round(sum(event_seq) / nullif(count(case when page = '/home' then 1 end), 0), 2) as event_seq_per_home
from with_latest
group by cube (page, case when row_seq >= 9 then 'LARGE' else 'SMALL' end)
order by search_rows desc
