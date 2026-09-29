select customer_id, country, upper(device) as device_upper from sessions order by country, device_upper
