select count(case when event_type = 'add_to_cart' then 1 end) as add_to_cart_count from web_events where duration_sec <> 398;
