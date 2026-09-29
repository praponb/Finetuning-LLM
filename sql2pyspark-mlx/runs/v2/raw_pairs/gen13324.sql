select id, page, event_type, case when event_type = 'purchase' then 1 else 0 end as is_purchase from web_events where page in ('/product', '/home');
