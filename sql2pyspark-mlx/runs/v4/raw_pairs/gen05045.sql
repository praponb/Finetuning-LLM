with base as (
    -- Basic cleansing and defaults
    select *, substring(browser, 1, 3) as browser_prefix
    from sessions
    where started_at > date '2024-01-30'
      and customer_id = 16
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by country, browser order by started_at desc, id) as event_seq,
        row_number() over (partition by country, browser order by started_at, id) as event_seq_2
    from base
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device order by event_seq, id) as latest_rank
        from with_windows
    ) ranked
    where latest_rank = 1
),
with_windows_sessions as (
    -- Ranking and gap features
    select
        *,
        round(event_seq / sum(event_seq) over (partition by country), 4) as event_seq_share,
        lead(event_seq, 2) over (partition by device, country order by event_seq, id) as next_event_seq,
        avg(event_seq) over (partition by browser order by event_seq_2, id rows between 3 preceding and 1 preceding) as event_seq_avg_3_prior
    from latest
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.web_event_event_type,
        top1.web_event_page
    from with_windows_sessions s
    left join lateral (
            select c.event_type as web_event_event_type, c.page as web_event_page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        p.credit_limit,
        p.signup_date
    from latest_match a
    join customers p
        on a.customer_id = p.id
)
select
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.browser_prefix,
    s.event_seq_2,
    s.event_seq_share,
    s.next_event_seq,
    (select avg(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as avg_order_discount
from enriched s
order by avg_order_discount, event_seq_2 desc
