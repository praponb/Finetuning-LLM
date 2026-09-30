with eligible as (
    select s.*
    from sessions s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time < s.started_at
        )
),
not_flagged as (
    select s.*
    from eligible s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
),
gaps as (
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval '90' day then 1 else 0 end as new_group_flag
    from not_flagged
),
sessionized as (
    select *, sum(new_group_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
valid as (
    select
        id,
        customer_id,
        device,
        new_group_flag,
        burst_id,
        date_format(started_at, 'yyyy-MM') as started_at_label
    from sessionized
    where country is not distinct from 'US'
      and customer_id is not null
),
with_latest as (
    select
        s.id,
        s.device,
        s.started_at_label,
        top1.page,
        top1.web_event_duration_sec
    from valid s
    join lateral (
            select c.page, c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
filtered as (
    select
        id,
        device,
        started_at_label,
        page,
        web_event_duration_sec,
        length(page) as page_length,
        coalesce(page, 'NONE') as page_clean
    from with_latest
    where page is not null
)
select
    device,
    page,
    case when page_length >= 61 then 'PREMIUM' else 'STANDARD' end as page_length_tier,
    min(page_length) as min_page_length,
    sum(case when page = '/product' then web_event_duration_sec else 0 end) as product_web_event_duration_sec,
    count(distinct id) as distinct_ids
from filtered
group by grouping sets ((device, page, case when page_length >= 61 then 'PREMIUM' else 'STANDARD' end), (device, page), (device))
order by page_length_tier nulls first;
