SELECT we.page, we.event_time, we.event_type, s.country FROM web_events we RIGHT OUTER JOIN sessions s ON we.session_id = s.id WHERE we.event_type NOT IN ('click', 'purchase')
