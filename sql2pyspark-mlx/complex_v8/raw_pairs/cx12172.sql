WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests,
        CONCAT(plan, '#', CAST(birth_year AS STRING)) AS plan_ref
    FROM listeners
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        signup_date,
        interests,
        LENGTH(TRIM(plan_ref)) AS plan_ref_len
    FROM cleaned
    WHERE SIZE(interests) >= 1
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
        )
)
SELECT id
FROM filtered
ORDER BY id
LIMIT 20
