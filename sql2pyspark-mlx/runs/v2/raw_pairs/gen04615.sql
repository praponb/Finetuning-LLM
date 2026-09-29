select id, started_at, trim(device) as clean_device, cast(started_at as date) as started_at_day from sessions where started_at < '2025-07-06' order by started_at_day, 2 desc
