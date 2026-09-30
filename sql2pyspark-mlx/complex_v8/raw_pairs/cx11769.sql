WITH valid AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        country,
        birth_year,
        SIZE(interests) AS interests_count
    FROM listeners
),
unique_rows AS (
    -- Keep the earliest row per country, plan
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country, plan ORDER BY birth_year, id) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.ms_played >= 62294
        )
)
SELECT
    id,
    plan,
    country,
    birth_year,
    interests_count
FROM qualified
ORDER BY country, birth_year, id
LIMIT 10
