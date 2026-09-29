SELECT
    id,
    country,
    started_at,
    browser
FROM sessions
WHERE customer_id IN (23, 31, 34, 35) AND device <> 'mobile' AND country = 'US'
ORDER BY browser DESC, id DESC;
