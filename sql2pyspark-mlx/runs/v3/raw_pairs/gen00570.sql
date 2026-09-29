select id, browser, initcap(trim(device)) device_title, date_trunc('year', started_at) started_at_period from sessions
