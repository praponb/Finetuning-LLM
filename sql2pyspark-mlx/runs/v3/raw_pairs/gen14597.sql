select event_type, upper(page) as page_upper, date_format(event_time, 'yyyy-MM') as event_time_label from web_events order by page_upper desc
