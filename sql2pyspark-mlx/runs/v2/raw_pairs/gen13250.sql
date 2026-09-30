SELECT
    id,
    country,
    CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg,
    CASE WHEN browser IN ('Edge', 'Chrome') THEN 'Priority' ELSE 'Other' END AS browser_group
FROM sessions
WHERE customer_id IS NULL
