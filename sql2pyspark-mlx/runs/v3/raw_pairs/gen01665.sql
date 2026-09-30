select id, page, concat(event_type, ' - ', page) as event_type_page from web_events where page in ('/cart', '/checkout') order by event_type_page desc, id limit 10;
