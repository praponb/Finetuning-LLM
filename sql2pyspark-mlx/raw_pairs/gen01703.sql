SELECT
    id,
    age,
    credit_limit,
    SUBSTRING(segment, 1, 3) AS segment_prefix,
    LOWER(name) AS name_lower
FROM customers
WHERE segment = 'Corporate';
