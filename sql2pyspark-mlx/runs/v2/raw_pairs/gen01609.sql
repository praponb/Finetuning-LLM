SELECT
    id,
    country,
    CONCAT(country, ' ', browser) AS country_browser,
    CASE WHEN device IN ('desktop', 'mobile') THEN 'Tier 1' ELSE 'Other' END AS device_group
FROM sessions;
