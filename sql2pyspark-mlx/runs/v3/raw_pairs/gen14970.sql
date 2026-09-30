select *, date_format(event_time, 'MMM yyyy') event_time_label from web_events where event_type in ('click', 'search') and page <> '/checkout' order by duration_sec
