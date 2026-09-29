select 'customer' as kind, country from customers where country is not null union select 'web_event' as kind, page from web_events where page is not null
