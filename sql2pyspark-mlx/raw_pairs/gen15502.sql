WITH matched_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM customers
    EXCEPT ALL
    SELECT city, id
    FROM customers
    WHERE UPPER(TRIM(email)) = 'JACK@STARTUP.IO'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    id,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    age,
    CASE WHEN country RLIKE '^(SG|DE)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS country_zone
FROM reconciled;
