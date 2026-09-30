select id, page, duration_sec, row_number() over (partition by page order by duration_sec, id) as rn from web_events where duration_sec between 210 and 216 order by rn desc
