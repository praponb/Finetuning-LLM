WITH prepared AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE country = 'UK'
)
SELECT id AS entity_id, plan
FROM prepared
UNION
SELECT track_id, source
FROM plays
ORDER BY entity_id DESC, plan
