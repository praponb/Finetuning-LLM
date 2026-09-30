select id, country, customer_id from sessions where hour(started_at) between 10 and 13 and country in ('UK', 'SG', 'DE') and customer_id is not null;
