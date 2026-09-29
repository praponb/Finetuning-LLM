with filtered as (
    select
        id,
        device,
        browser,
        started_at,
        country,
        coalesce(browser, 'N/A') as browser_filled
    from sessions
    where started_at >= to_timestamp('2023-08-23 15:00:00')
      and customer_id is not null
)
select
    s.id,
    s.started_at,
    s.browser_filled,
    latest.event_time
from filtered s
left join lateral (
        select c.event_time
        from web_events c
        where c.session_id = s.id
          and c.event_time <= s.started_at
        order by c.event_time desc, c.id desc
        limit 1
    ) latest on true;
