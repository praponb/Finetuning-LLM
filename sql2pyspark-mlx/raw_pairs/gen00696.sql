SELECT started_at, customer_id, country FROM sessions WHERE NOT (country IN ('TH', 'JP', 'DE') AND started_at >= '2024-02-01');
