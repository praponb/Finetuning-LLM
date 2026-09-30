SELECT
    id,
    started_at,
    customer_id,
    browser,
    CASE WHEN country IN ('TH', 'UK') THEN 'Core' ELSE 'Other' END AS country_group
FROM sessions
WHERE browser IN ('Edge', 'Chrome', 'Safari')
