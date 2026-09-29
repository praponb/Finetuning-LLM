select country, browser from sessions where hour(started_at) between 12 and 15 union all select country, browser from sessions where to_date(started_at) < '2025-07-01';
