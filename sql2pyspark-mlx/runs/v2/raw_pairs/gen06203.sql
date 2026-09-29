SELECT
    id,
    started_at,
    device,
    COALESCE(browser, 'Unknown') AS browser_filled
FROM sessions
WHERE customer_id IS NULL;
