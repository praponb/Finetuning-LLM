WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE is_verified = FALSE
)
SELECT debut_year AS entity_id
FROM base
EXCEPT
SELECT birth_year AS entity_id
FROM listeners
