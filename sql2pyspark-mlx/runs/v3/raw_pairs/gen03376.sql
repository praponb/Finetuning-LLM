select id, browser, device, to_date(started_at) started_at_date from sessions where browser is not null and started_at >= '2025-04-01' order by browser asc;
