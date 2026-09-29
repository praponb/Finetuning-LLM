WITH base AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        signup_date,
        interests
    FROM listeners
    WHERE birth_year <= 1994
      AND country IN ('TH', 'BR', 'KR')
),
combined AS (
    -- Stack the branches
    SELECT plan, country AS category
    FROM listeners
    WHERE country = 'TH'
    UNION ALL
    SELECT genre, country
    FROM artists
)
SELECT plan, COUNT(*) AS n_rows
FROM combined
GROUP BY plan
ORDER BY plan
