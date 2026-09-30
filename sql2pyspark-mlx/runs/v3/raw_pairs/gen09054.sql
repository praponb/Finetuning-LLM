select id, page, duration_sec from (select id, page, duration_sec, row_number() over (partition by page order by duration_sec, id) as pos from web_events) as ranked where pos <= 1;
