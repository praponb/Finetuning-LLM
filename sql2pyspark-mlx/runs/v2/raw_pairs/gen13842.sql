SELECT id, country FROM sessions WHERE device NOT IN ('mobile', 'tablet', 'desktop') UNION ALL SELECT id, country FROM sessions WHERE device <> 'mobile' ORDER BY id DESC;
