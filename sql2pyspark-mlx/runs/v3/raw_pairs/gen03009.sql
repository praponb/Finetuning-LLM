select id, browser, started_at, date_trunc('week', started_at) as started_at_period from sessions where hour(started_at) between 12 and 15 and customer_id is not null;
