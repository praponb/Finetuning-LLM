SELECT
    id,
    customer_id,
    CONCAT_WS('-', browser, country) AS browser_country_key,
    SUBSTRING(browser, 1, 3) AS browser_prefix
FROM sessions
WHERE country = 'DE';
