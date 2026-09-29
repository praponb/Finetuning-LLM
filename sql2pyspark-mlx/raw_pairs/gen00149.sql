with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        to_date(started_at) as started_at_date,
        coalesce(browser, 'OTHER') as browser_clean
    from sessions
    where started_at >= to_timestamp('2023-07-20 03:00:00')
      and started_at < '2025-01-01'
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.web_event_page
    from base s
    left join lateral (
            select c.page as web_event_page
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        substring(browser, 1, 3) as browser_prefix,
        coalesce(country, 'OTHER') as country_clean
    from with_last
    where started_at > date '2023-03-19'
      and web_event_page is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by browser order by started_at_date desc nulls last) as latest_rank
        from filtered
    ) x
    where latest_rank = 1
)
-- Running and moving aggregates
select
    id,
    customer_id,
    device,
    browser,
    started_at,
    started_at_date,
    browser_clean,
    web_event_page,
    row_number() over (partition by customer_id order by started_at asc, id asc) as row_seq,
    first_value(web_event_page) ignore nulls over (partition by web_event_page order by latest_rank asc, id asc rows between unbounded preceding and current row) as first_known_web_event_page,
    row_number() over (partition by web_event_page order by started_at desc, id asc) as seq_no,
    row_number() over (partition by date_trunc('week', started_at_date), browser order by started_at_date desc, id asc) as event_seq,
    first_value(country, true) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_country
from latest
where browser <> 'Edge';
