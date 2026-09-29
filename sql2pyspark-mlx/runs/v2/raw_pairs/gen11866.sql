select id, page, session_id, date_format(event_time, 'yyyy-MM-dd') as event_time_label from web_events where page = '/checkout' and duration_sec > 170;
