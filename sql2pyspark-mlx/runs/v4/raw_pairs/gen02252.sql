SELECT
    id,
    is_active,
    city,
    country,
    CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'Core' ELSE 'Other' END segment_group
FROM customers
WHERE email LIKE '%@company.co';
