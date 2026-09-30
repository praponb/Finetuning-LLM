SELECT device, customer_id, started_at, COALESCE(browser, 'Unknown') AS browser_filled FROM sessions;
