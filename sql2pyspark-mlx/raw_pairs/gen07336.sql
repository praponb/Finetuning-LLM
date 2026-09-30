select end_date as event_date, 'subscription' as record_type from subscriptions union select opened_date as event_date, 'store' as record_type from stores order by event_date
