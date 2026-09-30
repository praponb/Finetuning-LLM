SELECT id, country, customer_id, LOWER(browser) AS browser_lower FROM sessions WHERE device IN ('desktop', 'mobile') AND country IN ('DE', 'SG') AND started_at >= '2023-04-18';
