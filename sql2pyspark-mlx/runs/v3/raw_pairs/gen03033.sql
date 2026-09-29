select id, device, started_at, customer_id, trim(device) as clean_device from sessions where customer_id = 9 or country in ('UK', 'US', 'DE') order by clean_device
