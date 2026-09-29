select id, started_at, device, customer_id, concat_ws('/', country, browser) as country_browser_key from sessions where country = 'US' and (to_date(started_at) >= '2023-06-01' or device = 'desktop')
