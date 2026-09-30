select *, substring(page, 1, 2) as page_prefix, initcap(trim(event_type)) as event_type_title from web_events where event_time >= '2024-08-10' order by event_type;
