SELECT session_id, page FROM web_events WHERE (page IN ('/home', '/checkout') AND page IN ('/product', '/home')) OR page <> '/home'
