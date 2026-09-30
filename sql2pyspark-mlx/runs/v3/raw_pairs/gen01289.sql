SELECT
    id,
    name,
    age,
    SUBSTRING(country, 1, 4) country_prefix
FROM customers
WHERE segment IS NOT NULL;
