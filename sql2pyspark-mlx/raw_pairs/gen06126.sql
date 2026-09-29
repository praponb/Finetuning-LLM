WITH filtered AS (
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        IF(is_active, 'Yes', 'No') AS is_active_label,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE segment IS NOT NULL
      AND city IS DISTINCT FROM 'Berlin'
)
SELECT segment, city
FROM filtered
INTERSECT
SELECT segment, city
FROM customers
WHERE UPPER(TRIM(email)) = 'KANYA@MAIL.CO.TH';
