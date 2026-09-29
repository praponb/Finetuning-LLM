select country, browser, substring(device, 1, 3) as device_prefix from sessions where to_date(started_at) < '2023-12-01';
