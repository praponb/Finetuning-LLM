WITH prepared AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        interests,
        LAST_DAY(signup_date) AS signup_date_month_end
    FROM listeners
    WHERE (signup_date BETWEEN DATE '2024-08-01' AND DATE '2025-07-08' OR UPPER(TRIM(username)) LIKE 'A%')
)
SELECT country AS label, username AS name, 'listener' AS record_type
FROM prepared
WHERE LOWER(username) LIKE '%99%'
  AND ARRAY_CONTAINS(interests, 'jazz')
UNION ALL
SELECT artist_name AS label, country AS name, 'artist' AS record_type
FROM artists
WHERE country = 'US'
