SELECT
    id,
    credit_limit,
    name,
    is_active,
    CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'Priority' ELSE 'Other' END AS segment_group
FROM customers
WHERE signup_date < '2024-09-11';
