with with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, top1.event_type
    from sessions s
    join lateral (
            select c.event_type
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    device,
    browser,
    country,
    event_type,
    case when device rlike '^(desktop|mobile)$' then 'OTHER_ZONE' else 'PRIORITY' end as device_zone
from with_last;
