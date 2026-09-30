SELECT
    id,
    segment,
    email,
    CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk,
    REGEXP_REPLACE(name, ' ', '_') AS name_slug
FROM customers
WHERE MONTH(signup_date) IN (2, 3, 5) AND (email NOT LIKE '%@mail.co.th' OR age IS NOT NULL)
ORDER BY id DESC
LIMIT 10
