WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE status = 'LAPSED'
)
SELECT id AS qty, status_slug AS label
FROM recent_policys
MINUS
SELECT id AS qty, status AS label
FROM claims
ORDER BY qty, label
