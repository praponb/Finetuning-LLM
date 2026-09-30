SELECT *, UPPER(event_type) AS event_type_upper FROM web_events WHERE page = '/product' AND event_type NOT IN ('page_view', 'click', 'search') AND event_time >= '2023-12-04'
