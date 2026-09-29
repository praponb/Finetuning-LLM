select id, started_at, customer_id, concat_ws('-', country, browser) country_browser_key from sessions where customer_id is not null and country = 'UK';
