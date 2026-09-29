SELECT * FROM (SELECT id, customer_id, started_at, browser, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS pos FROM sessions) AS ranked WHERE pos = 1;
