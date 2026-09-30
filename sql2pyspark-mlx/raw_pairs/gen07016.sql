select *, initcap(trim(event_type)) as event_type_title from web_events where duration_sec is not null order by page, id limit 1;
