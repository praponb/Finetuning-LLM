SELECT
    id,
    browser,
    device,
    country
FROM sessions
WHERE country IN ('DE', 'TH');
